'use strict';
// Global Modules
var uuid = require('node-uuid');
var merge = require('merge');
// Local Modules
var UTIL = require('./UTIL.js');
var Parallel = require('paralleljs');

//--- PERSISTENCE LAYER

module.exports = function() {
  var INDEXER = new require('./INDEXER.js')();

  /* NOTE:
    When we delete records, we mark their corresponding indices as available for overwrite by new records,
    so we never have to rewrite the whole file to remove any records, sparing disk usage.
    However, this means that records won't always be stored in the order of which they were added.
    These available indices will be used in storing new records/docs instead of 'appending', except for batch inserts.

    NOTE:
    We only need to flush if we're going to need to swap in from file to read the next partition,
    this is to mitigate the risk of losing visible changes in the current partition when we cycle back to load it again.
    This is because the collection is modified in place, and changes are queued to be written to disk.
    Loading this partition again means changes might not be written yet, and we get an old copy.
  */

  // Journaling Module (abstraction between persistence and the file storage module)
  // NOTE The journal works on a byte and doc level abstraction
  // This module only works on doc level
  var JOURNAL; // gets initialized in loadCollection()

  var TOTAL_DOCS = 0; // how many docs are in the db
  var PARTITION = 0; // partition no. currently loaded into collection
  var PARTITION_LOADED_SIZE = 0; // size of loaded partition = no. docs (excluding new docs)
  var PARTITION_CURRENT_SIZE = 0; // size of loaded partition = no. docs + new docs
  var PARTITION_MAX = 0; // max size of loaded partition = no. docs NOTE Redundant with LOADED_SIZE, gets initialized in loadCollection()
  var PARTITION_OFFSET = 0; // starting byte offset of currently loaded partition

  var MAX_COLLECTION_SIZE = 256000; //2048000;

  var DOC_SIZE = 4096;
  var END_OFFSET = TOTAL_DOCS - PARTITION_LOADED_SIZE; // offset of last partition in no. docs (NOTE: Used for determining _blki on inserts, blki's can be changed on write to disk when free space (free blocks) is reused)

  var PERSISTENCE = {}; // returned via exports
  PERSISTENCE.COLLECTION = [];

  PERSISTENCE.saveCollection = function (fd, collection, callback) {
      //console.log('=> Saving - Filtering null records...');
      //console.time('<=> Filter Null Records Time');
      //UTIL.filterDeleted(PERSISTENCE.COLLECTION);
      //console.timeEnd('<=> Filter Null Records Time');
      console.log('Persistence - Saving changes to any of', TOTAL_DOCS + ' docs (' + PERSISTENCE.COLLECTION.length + ' loaded)');
      JOURNAL.flush();
      //FILER.saveFileStream(fd, collection, callback);
  }

  PERSISTENCE.loadCollection = function (fd, callback) {
      //FILER.loadFileStream(fd, callback);
      JOURNAL = new require('./JOURNAL.js')(fd);
      PARTITION_MAX = JOURNAL.PARTITION_MAX;

      console.time('Persistence - Load Time');
      var cycles = 0; // number of paritions gone through
      var noPartitions = JOURNAL.getNoPartitions();
      if(this.getTotalDocs() > MAX_COLLECTION_SIZE)
          noPartitions = 1; // force us to load only one partition / the first one
      console.log('Persistence - No. Partitions', noPartitions);
      for(var i = 0; i < noPartitions; i++) { // for each parition, load until we can't anymore
          if(cycles < noPartitions) {
              JOURNAL.load(function(err, collection, totalDocs, partitionIndex, partitionStart, partitionSize, partitionMax) {
                  // journal loads partition containing all records we can fit
                  if(collection) {
                      if(collection.length + PERSISTENCE.COLLECTION.length < MAX_COLLECTION_SIZE) {
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      } else {
                          PERSISTENCE.COLLECTION.splice(0, collection.length);
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      }
                  }

                  console.timeEnd('Persistence - Load Time');
                  // NOTE: We receive these vars from JOURNAL each time we load a partition
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PERSISTENCE.COLLECTION.length;
                  PARTITION_MAX = partitionMax || 0;
                  //PARTITION_OFFSET = partitionStart || 0;
                  PARTITION_OFFSET = PERSISTENCE.COLLECTION[0]._blki || 0;
                  END_OFFSET = MAX_COLLECTION_SIZE - collection.length;
                  //PARTITION_OFFSET = END_OFFSET || 0;
                  console.log('Persistence - Total Docs in DB', TOTAL_DOCS + ' Partition Index', PARTITION  + ' Partition Start', PARTITION_OFFSET + ' LOADED', PARTITION_LOADED_SIZE + ' docs');
                  cycles++;
              });
          }
      }
      callback(false); // no error
  }

  PERSISTENCE.getLoadedDocs = function() {
      return PERSISTENCE.COLLECTION.length;
  }

  PERSISTENCE.getTotalDocs = function() {
      return JOURNAL.getTotalDocs();
  }

  // TODO: If obj has _id, check if it exists via index on _id, and check form of _id
  // Add the _id property to each object and check if it should be indexed
  PERSISTENCE.addIDProperty = function (obj) {
      if(obj.length) { // assuming an array
          for(var i = 0; i < obj.length; i++) {
              if(!obj[i]['_id'])
                  obj[i]._id = uuid.v4().replace(/-/g, '');
              for(var p in INDEXER.INDICES) // check if new object contains a field to index on
                  if(obj[p]) // ok there's a field to index on
                      INDEXER.add(p, obj); // index this record
          }
      } else { // single object
          if(!obj['_id'])
              obj._id = uuid.v4().replace(/-/g, '');
          for(var p in INDEXER.INDICES) // check if new object contains a field to index on
              if(obj[p]) // ok there's a field to index on
                  INDEXER.add(p, obj); // index this record
      }
      return obj;
  }

  PERSISTENCE.createIndex = function(field, collection) {
      if(typeof(field) === 'object' && field.length) {
          for(var i = 0; i < field.length; i++) {
              if(INDEXER.INDICES[field[i]]) {
                  field.splice(i, 1);
                  i--;
              }
          }
          if(field.length === 0) {
              return false; // all specified indices have been built
          }
      } else {
          if(INDEXER.INDICES[field])
              return false; // specified index has been built
      }

      JOURNAL.flush(); // flush before all changes because building an index, because we cycle through partition data

      console.log('Persistence - Building index on', field);
      if(this.COLLECTION.length >= TOTAL_DOCS) {
          INDEXER.build(field, this.COLLECTION);
          console.log('=> Built index on', field, 'Total Index Size', INDEXER.getSize(field));
          return true;
      }

      var cycles = 1; // number of paritions gone through, we start with one because at-least one is already loaded by loadCollection()
      var noPartitions = JOURNAL.getNoPartitions();
      for(var i = 0; i < noPartitions; i++) { // for each parition, expand the index
          INDEXER.build(field, this.COLLECTION.slice(END_OFFSET, this.COLLECTION.length));
          console.log('=> Partially built index on', field + ' in Partition', PARTITION + ' Current Index Size', INDEXER.getSize(field));
          console.log('=> Loading next partition', (PARTITION+1 < noPartitions ? PARTITION+1 : cycles) + ' Can load next?', cycles < noPartitions);
          if(cycles < noPartitions) {
              JOURNAL.load(function(err, collection, totalDocs, partitionIndex, partitionStart, partitionSize, partitionMax) {
                  // journal loads partition containing all records we can fit
                  if(collection) {
                      if(collection.length + PERSISTENCE.COLLECTION.length < MAX_COLLECTION_SIZE)
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      else {
                          PERSISTENCE.COLLECTION.splice(0, collection.length);
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      }
                  }

                  console.timeEnd('Persistence - Load Time');
                  // NOTE: We receive these vars from JOURNAL each time we load a partition
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PERSISTENCE.COLLECTION.length;
                  PARTITION_MAX = partitionMax || 0;
                  //PARTITION_OFFSET = partitionStart || 0;
                  PARTITION_OFFSET = PERSISTENCE.COLLECTION[0]._blki || 0;
                  END_OFFSET = MAX_COLLECTION_SIZE - collection.length;
                  //PARTITION_OFFSET = END_OFFSET || 0;
                  /*
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PARTITION_LOADED_SIZE;
                  PARTITION_MAX = partitionMax || 0;
                  PARTITION_OFFSET = partitionStart || 0;
                  END_OFFSET = TOTAL_DOCS - PARTITION_LOADED_SIZE;
                  */
                  console.log('Persistence - Total Docs in DB', TOTAL_DOCS + ' Partition Index', PARTITION  + ' Partition Start', PARTITION_OFFSET + ' LOADED', PARTITION_LOADED_SIZE + ' docs');
                  cycles++;
              });
          }
      }
      //INDEXER.build(field, PERSISTENCE.COLLECTION);
      console.log('=> Built index on', field, 'Total Index Size', INDEXER.getSize(field));
      return true;
  }

  PERSISTENCE.destroyIndex = function (field) {
      return INDEXER.destroy(field);
  }

  PERSISTENCE.indexSize = function (field) {
      return INDEXER.getSize(field);
  }

  PERSISTENCE.indexDeepSize = function (field) {
      return INDEXER.deepSize(field);
  }

  PERSISTENCE.getIndices = function () {
      return INDEXER.getIndices();
  }

  PERSISTENCE.getNoIndices = function () {
      return INDEXER.getNoIndices();
  }

  /*
    data can be an object or an array of objects (1D or 2D array)
  */
  PERSISTENCE.inserter = function(collection, data) {
      var inserted = 0;
      var array = false;
      if(data.length) { // assuming an array of objects
          var obj = {};
          for(var i = 0; i < data.length; i++) {
              obj = data[i];
              // check for [[obj,obj,],] aka array of arrays
              if(obj.length && typeof obj === 'object') { // array of objects (hopefully) in the array
                  collection.concat(obj.forEach(function(o) {
                      PERSISTENCE.addIDProperty(o);
                  }));
                  inserted += obj.length;
                  var doc = {};
                  for(var j = 0; j < inserted; j++) {
                      //if(!collection[ (collection.length-inserted) + j + i ]['_blki'])
                      doc = collection[ (collection.length-inserted) + j + i ]; // get object from collection
                      doc._blki = (END_OFFSET + i + j); // set it's blki
                      JOURNAL.insert( // will perform a batch insert of this sub-array
                        doc
                      );
                      // check if we should update any index for this record
                      for(var p in INDEXER.INDICES) {
                          if(doc[p]) { // this field is indexed
                              //console.log('UTIL.inserter Updating indexed key', p);
                              // Params: field, key, val (field is the indexed property name)
                              // Key is the value of that property, and val is block index pointing to the record
                              INDEXER.add(p, doc._blki);
                          }
                      }
                  }
              } else if(typeof obj === 'object') { // single object in array
                  if(collection.length < MAX_COLLECTION_SIZE)
                      collection.push(this.addIDProperty(obj));
                  this.addIDProperty(obj);
                  inserted++;
                  //if(!obj._blki) // new records should never have _blki's
                  obj._blki = (END_OFFSET + i);
                  // check if we should update any index for this record
                  for(var p in INDEXER.INDICES) {
                      if(obj[p]) { // this field is indexed
                          //console.log('UTIL.inserter Adding indexed key', p);
                          // Params: field, key, val (field is the indexed property name)
                          // Key is the value of that property, and val is block index pointing to the record
                          INDEXER.add(p, obj[p], obj._blki);
                      }
                  }
                  // using batch insert for arrays of docs
                  array = true;
              } else {
                  // invalid data encountered
                  console.error('Persistence - Error in record(s) to insert!');
              }
          }
          if(array) { // batch insert to journal
              JOURNAL.insert(
                data // batch insert, always appends
              );
          }
      } else { // single object (data = doc object)
          var index = 0;
          if(JOURNAL.FREE_BLOCKS.length > 0) {
              index = JOURNAL.FREE_BLOCKS.shift();
              JOURNAL.REUSED_BLOCKS++;
              data._blki = index;
          } else {
              index = collection.length;
              // NOTE we need to recalculate the blki field in the doc since we're actually inserting it soewhere in the middle
              data._blki = (END_OFFSET + index);
          }

          if(collection.length < MAX_COLLECTION_SIZE) {
              if(PARTITION_OFFSET < data._blki == data._blki < PARTITION_OFFSET+collection.length) {// index falls into loaded partition range
                  collection[index] = this.addIDProperty(data); // replace deleted record in currently loaded collection
              } else { // append
                  if(collection.length < MAX_COLLECTION_SIZE)
                      collection.push(this.addIDProperty(data));
              }
          }

          // check if we should update any index for this record
          for(var p in INDEXER.INDICES) {
              if(data[p]) { // this field is indexed
                  //console.log('UTIL.inserter Adding indexed key', p);
                  // Params: field, key, val (field is the indexed property name)
                  // Key is the value of that property, and val is block index pointing to the record
                  INDEXER.add(p, data[p], data._blki);
              }
          }

          inserted++;
          JOURNAL.insert( // add obj and it's blki into journal insert queue
            data
          );
      }
      if(inserted > 0) {
          TOTAL_DOCS += inserted;
          PARTITION_CURRENT_SIZE += inserted;
          END_OFFSET = TOTAL_DOCS - PARTITION_LOADED_SIZE;
      }
      return inserted;
  }

  /*
    Note, if you search for a whole object and any of it's fields match
    another objects respective field, it will be returned in the array.
    Even if no other fields match.
    Unless you pass matchAllFields = true
    If multi = false, the first matching object will be returned
    Note:
      Indexed searches are faster by roughly a factor of O(numQueryFields / numIndexedFields),
      when doing a multi search on one or more indexed and none indexed fields.
  */
  function processIndexed(blki) {
      var rec = {};
      var match = false;
      var retVal = {
          match: false,
          block: -1
      };

      // Check if blki falls within loaded doc range
      if(TOTAL_DOCS <= MAX_COLLECTION_SIZE) { // NOTE When whole collection is loaded
          if(!(0 < blki == blki < PERSISTENCE.COLLECTION.length)) {
              retVal.block = blki;
          }
      } else { // NOTE When whole collection is not loaded
          if(!(PARTITION_OFFSET < blki == blki < PARTITION_OFFSET+PERSISTENCE.COLLECTION.length)) {
              retVal.block = blki;
          } else {
              blki = blki - PARTITION_OFFSET;
          }
      }

      if(retVal.block === -1) {
          // blki falls within currently loaded range
          rec = PERSISTENCE.COLLECTION[blki];

          if(matchAll)
              match = UTIL.matchAll(rec, q);
          else
              match = UTIL.matchAny(rec, q);
          if(match) {
              retVal.match = true;
              if(!multi) {
                  console.log('=> Found One via index');
              }
          }
      }

      return retVal;
  }

  PERSISTENCE.finder = function(collection, query, multi, matchAll, options, retD, docsR) {
      var retDocs = retD || [];
      var docsRead = docsR || 0;
      var rec = {};
      var match = false; // whether or not a record matches the query
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      var indexedFields = 0; // no. of fields in query that are indexed NOTE Not used yet
      var blockIndices = []; // block indices to get from file that aren't in the collection range, after a full index traversal of indexed fields

      console.log('Persistence - Finding with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              indexedFields++; // NOTE Not used yet
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs && indexedRecs.length > 0) {
                  if(typeof(indexedRecs[0]) === 'object' && indexedRecs[0].length) { // assume 2D array
                      console.error('=> WARNING: Unexpectedly forced to flatten index results array! Index might be broken.');
                      indexedRecs = [].concat.apply([], indexedRecs); // flatten the 2D array
                  }
                  console.log('=> Indexed Docs', indexedRecs.length);
                  //console.log(indexedRecs);
                  // we retrieve from memory first, then file, after index is done being read, and we have all the blki's
                  //var blki = 0;

                  var environment = {
                      UT: UTIL,
                      T_D: TOTAL_DOCS,
                      M_C_S: MAX_COLLECTION_SIZE,
                      P_O: PARTITION_OFFSET,
                      mA: matchAll,
                      mu: multi,
                      coll: collection,
                      q: query,
                      fi: p
                  };

                  var para = new Parallel(indexedRecs.splice(0,4));

                  para.require(processIndexed);

                  para.map(function(blki) {
                      //var blki = doc._blki;
                      return processIndexed(blki);
                  }).then(function(doc) {
                      console.log(doc);
                  });

                  while(true){}

                  /*for(var i = 0; i < indexedRecs.length; i++) {
                      blki = indexedRecs[i];
                      // NOTE We skip a loop cycle when the blki is not in loaded docs range so that we don't attempt to fetch it from memory.
                      // We then queue it up for file read after the index search is done
                      if(TOTAL_DOCS <= MAX_COLLECTION_SIZE) { // NOTE When whole collection is loaded
                          if(!(0 < blki == blki < collection.length)) {
                              //console.log('Not in range', blki);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1) { // NOTE Slows us down on large return arrays
                                  blockIndices.push(blki);
                                  //indexedRecs.splice(0, 1);
                                  //i--; // we skip a counter when splicing in the same direction as the loop
                              }
                              continue; // next loop cycle
                          }
                      } else { // NOTE When whole collection is not loaded
                          if(!(PARTITION_OFFSET < blki == blki < PARTITION_OFFSET+collection.length)) {
                              //console.log('Not in range', blki, PARTITION_OFFSET, PARTITION_OFFSET+collection.length);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1) { // NOTE Slows us down on large return arrays
                                  blockIndices.push(blki);
                                  //indexedRecs.splice(0, 1);
                                  //i--; // we skip a counter when splicing in the same direction as the loop
                              }
                              continue; // next loop cycle
                          } else {
                              blki = blki - PARTITION_OFFSET;
                          }
                      }
                      // blki falls within currently loaded range
                      //console.log('In range', blki + ' Index', blki-PARTITION_OFFSET);
                      //rec = collection[blki-PARTITION_OFFSET];
                      rec = collection[blki];

                      if(!rec) {
                          console.error('=> Skipping Invalid record at BLKI', blki + ' Index', blki-PARTITION_OFFSET, i);
                          continue;
                      }

                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchAny(rec, query);
                      if(match) {
                          // NOTE Not needed if we filter by blki's during offset check
                          //console.log('!!! Match via collection', rec._blki);
                          // check if we've already found this object (multi index reference issue)
                          //if(UTIL.listContainsByID(retDocs, rec)) { // NOTE Slows us down on large return arrays
                              //console.log('UTIL.finder Already found!');
                            //continue; // next loop cycle
                          //}
                          retDocs.push(rec);
                          if(!multi) {
                              console.log('=> Found One via index:', p);
                              break; //return this.getUniqueElements(retDocs); // slower
                          }
                      }
                  }*/
                  console.log('=> Ret docs', retDocs.length);
              } else {
                  // NOTE: This means not whole index is in memory, so we cannot exclude this field from normal search
                  console.log('=> Index does not contain anything for', p, query[p]);
                  //keysNotIndexed.push(p);
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

      // NOTE
      // Now read from file all docs with blki's found in index but not in loaded memory
      // This is only helpful when not having to load more partitions and iterate over them normally...
      // If we have indexed docs in range outside of whats in memory, and we don't require a normal search
      // if(blockIndices.length > 0 && (keysNotIndexed.length === 0 || !multi))
      if(blockIndices.length > 0)
      {
          // NOTE Any unflushed changes cause finding inconsistent data in the partitions
          JOURNAL.flush();
          console.log('=> Getting from Journal', blockIndices.length + ' unique indexed docs (blkis)');
          // sort the block indices so we can get sequential access performance when reading from file
          UTIL.quickSort(blockIndices);
          // NOTE: Gets the records at the block indices, reading is a blocking operation
          indexedRecs = JOURNAL.loadBlki(blockIndices);
          //console.log(indexedRecs);
          for(var j = 0; j < indexedRecs.length; j++) {
              rec = indexedRecs[j];
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  // NOTE Not needed if we filter by blki's during offset check
                  //console.log('!!! Match via file', rec._blki);
                  //if(UTIL.listContainsByID(retDocs, rec)) { // NOTE Slows us down on large return arrays
                      //console.log('UTIL.finder Already found!');
                      //continue; // next loop cycle
                  //}
                  retDocs.push(rec);
                  if(!multi) {
                      console.log('=> Found One via index:', p);
                      break;
                  }
              }
          }
          console.log('=> Ret docs', retDocs.length);
      }

      //--- Ok we have fields to search on that were not indexed
      // This is due to matchAll AND a field not indexed
      // Or no indexed fields at all
      if(keysNotIndexed.length > 0) {
          if(!indexed) {
              console.log('=> No query field(s) are indexed!');
          } else {
              // only if any field can match, remove already used query keys
              // deleting a key is a very expensive operation from here so we don't do it
              // we build a new query instead with keys that were not indexed
              if(!matchAll) {
                  var newQuery = {};
                  for(var i = 0; i < keysNotIndexed.length; i++) {
                      newQuery[keysNotIndexed[i]] = query[keysNotIndexed[i]];
                  }
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + '\n=> to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
              docsRead++;
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  // check if we've already found this object (multi index reference issue)
                  if(UTIL.listContainsByBLKID(retDocs, rec._blki)) { // NOTE Slows us down on large return arrays
                      //console.log('UTIL.finder Already found!!');
                      continue; // next loop cycle
                  }
                  retDocs.push(rec);
                  if(!multi) {
                      break;
                  }
              }
          }
          console.log('=> Ret docs', retDocs.length);
          if((!match || multi) && docsRead < TOTAL_DOCS) {
              console.log('Persistence - Loading Next Partition for Search. Flushing first...');
              // NOTE We don't really need to flush here since we don't make changes during search
              // NOTE BUT any unflushed changes cause finding inconsistent data in the following partitions
              JOURNAL.flush();
              console.time('Persistence - Load Time');
              JOURNAL.load(function(err, collection, totalDocs, partitionIndex, partitionStart, partitionSize, partitionMax) {
                  // journal loads partition containing all records we can fit
                  if(collection)
                      if(collection.length + PERSISTENCE.COLLECTION.length < MAX_COLLECTION_SIZE)
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      else {
                          PERSISTENCE.COLLECTION.splice(0, collection.length);
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      }

                  console.timeEnd('Persistence - Load Time');
                  // NOTE: We receive these vars from JOURNAL each time we load a partition
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PERSISTENCE.COLLECTION.length;
                  PARTITION_MAX = partitionMax || 0;
                  //PARTITION_OFFSET = partitionStart || 0;
                  PARTITION_OFFSET = PERSISTENCE.COLLECTION[0]._blki || 0;
                  END_OFFSET = MAX_COLLECTION_SIZE - collection.length;
                  //PARTITION_OFFSET = END_OFFSET || 0;
                  console.log('Persistence - Total Docs in DB', TOTAL_DOCS + ' Partition Index', PARTITION  + ' Partition Start', PARTITION_OFFSET + ' LOADED', PARTITION_LOADED_SIZE + ' docs');

                  PERSISTENCE.finder(collection, query, multi, matchAll, options, retDocs, docsRead);
              });
          }
      }
      if(options && retDocs.length > 0) {
          // if we have a selection query, return only those fields from matching docs
          // TODO: Try modifying array in place by removing fields from each doc
          // TODO: Try selecting as documents are found, instead of after
          if(options.select && !multi) { // !multi, because this will break sort
              // select can either be a string or an array of strings
              console.log('=> Selecting', options.select);
              if(typeof(options.select) === 'string')
                  retDocs = UTIL.filterSelected(retDocs, [options.select]);
              else if(options.select.length > 0)
                  retDocs = UTIL.filterSelected(retDocs, options.select);
          } else {
              // don't bother attempting this on single doc searches
              if(multi) {
                  // sort before limiting, skipping or selecting
                  if(options.sort) {
                      var sort = options.sort;
                      // quick sort the array in place
                      if(typeof(sort) === 'object') { // check for sort:{field:order}
                          console.log('=> Sorting on', sort);
                          UTIL.quickSort(retDocs, sort); // quickSort is ascending
                          if(sort[Object.keys(sort)[0].toString()] === -1) { // we want descending order, get from first property/key's value
                              retDocs.reverse(); // reverse the array in place, very fast in v8
                          }
                      }
                  }
                  if(options.select) { // now we can select, skip and limit since we need only indices..
                      // select can either be a string or an array of strings
                      console.log('=> Selecting', options.select);
                      if(typeof(options.select) === 'string')
                          retDocs = UTIL.filterSelected(retDocs, [options.select]);
                      else if(options.select.length > 0)
                          retDocs = UTIL.filterSelected(retDocs, options.select);
                  }
                  // skip before limiting
                  if(options.skip && typeof(options.skip) === 'number') {
                      console.log('=> Skipping', options.skip);
                      // skip the first x elements by splicing them away
                      retDocs.splice(0, options.skip);
                  }
                  if(options.limit && typeof(options.limit) === 'number') {
                      console.log('=> Limiting', options.limit);
                      // splice the array so it contains the first x records, where x is options.limit
                      // if the limit is 2, start at index 2 and remove elements until the end
                      // keeping only the first 2 as specified by limit
                      retDocs.splice(options.limit, retDocs.length);
                  }
                  if(options.sum && typeof(options.sum) === 'string') {
                      console.log('=> Sum on', options.sum);
                      retDocs = [UTIL.sumOnField(retDocs, options.sum)]; // return the sum into an array ie: [sum]
                  }
              }
          }
      }
      console.log('Persistence - Found', retDocs.length + ' documents in', i + ' iterations');
      return retDocs; //this.getUniqueElements(retDocs); // slower
  }

  PERSISTENCE.updater = function(collection, query, data, multi, matchAll, docsR) {
      var docsRead = docsR || 0;
      var rec = {};
      var updated = 0;
      var match = false;
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      var blockIndices = []; // block indices to get from file that aren't in the collection range, after a full index traversal of indexed fields

      console.log('Persistence - Updating with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);
      console.log('Persistence - Updating with Data:', JSON.stringify(data));

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs && indexedRecs.length > 0) {
                  if(typeof(indexedRecs[0]) === 'object' && indexedRecs[0].length) { // assume 2D array
                      console.log('flattening index results array');
                      indexedRecs = [].concat.apply([], indexedRecs); // flatten the 2D array
                  }
                  console.log('=> Indexed Docs', indexedRecs.length);
                  //console.log(indexedRecs);
                  // we retrieve from memory first, then file, after index is done being read, and we have all the blki's
                  var blki = 0;
                  for(var i = indexedRecs.length-1; i >= 0; i--) {
                      blki = indexedRecs[i];
                      // NOTE We skip a loop cycle when the blki is not in loaded docs range so that we don't attempt to fetch it from memory.
                      // We then queue it up for file read after the index search is done
                      if(TOTAL_DOCS <= MAX_COLLECTION_SIZE) { // NOTE When whole collection is loaded
                          if(!(0 < blki == blki < collection.length)) {
                              //console.log('Not in range', blki);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1)
                                  blockIndices.push(blki);
                              continue; // next loop cycle
                          }
                      } else { // NOTE When whole collection is not loaded
                          if(!(PARTITION_OFFSET < blki == blki < PARTITION_OFFSET+collection.length)) {
                              //console.log('Not in range', blki, PARTITION_OFFSET, PARTITION_OFFSET+collection.length);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1)
                                  blockIndices.push(blki);
                              continue; // next loop cycle
                          } else {
                              blki = blki - PARTITION_OFFSET;
                          }
                      }
                      // blki falls within currently loaded range
                      //console.log('In range', blki + ' Index', blki-PARTITION_OFFSET);
                      rec = collection[blki-PARTITION_OFFSET];

                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchAny(rec, query);
                      if(match) {
                          // check if we should update any index for this record
                          for(var p in INDEXER.INDICES) {
                              if(rec[p] && data[p]) { // this field is indexed and changing
                                  //console.log('UTIL.updater Updating indexed key', p);
                                  // Params: field, rec[field], data[field], block_index, deleting
                                  INDEXER.update(p, data[p], data[p], rec._blki, false);
                              } else {
                                  //console.log('UTIL.updater Updating indexed key', p);
                                  // Params: field, rec[field], data[field], block_index, deleting
                                  INDEXER.update(p, rec[p], rec[p], rec._blki, false);
                              }
                          }
                          rec = merge(rec, data);
                          updated++;
                          JOURNAL.update(rec);
                          if(!multi) {
                              console.log('=> Updated One via index:', p);
                              break;
                          }
                      }
                  }
                  console.log('=> Updated docs', updated);
              } else {
                  // NOTE: This means not whole index is in memory, so we cannot exclude this field from normal search
                  console.log('=> Index does not contain anything for', p, query[p]);
                  keysNotIndexed.push(p);
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

      if(blockIndices.length > 0) // if we indexed docs in range outside of whats in memory
      {
          // NOTE Any unflushed changes cause finding inconsistent data in the partitions
          JOURNAL.flush();
          console.log('=> Getting from Journal', blockIndices.length + ' unique indexed docs (blkis) Updated', updated);
          // sort the block indices so we can get sequential access performance when reading from file
          UTIL.quickSort(blockIndices);
          // NOTE: Gets the records at the block indices, reading is a blocking operation
          indexedRecs = JOURNAL.loadBlki(blockIndices);
          // loop backwards due to INDEXER.update splicing stored values pointing to the record
          for(var j = indexedRecs.length-1; j >= 0; j--) {
              rec = indexedRecs[j];
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  // check if we should update any index for this record
                  for(var p in INDEXER.INDICES) {
                      if(rec[p] && data[p]) { // this field is indexed and changing
                          //console.log('UTIL.updater Updating indexed key', p);
                          // Params: field, rec[field], data[field], block_index, deleting
                          INDEXER.update(p, data[p], data[p], rec._blki, false);
                      } else {
                          //console.log('UTIL.updater Updating indexed key', p);
                          // Params: field, rec[field], data[field], block_index, deleting
                          INDEXER.update(p, rec[p], rec[p], rec._blki, false);
                      }
                  }
                  rec = merge(rec, data);
                  updated++;
                  JOURNAL.update(rec);
                  if(!multi) {
                      break;
                  }
              }
          }
          console.log('=> Updated docs', updated);
      }

      if(keysNotIndexed.length > 0) {
          if(!indexed) {
              console.log('=> No query field(s) are indexed!');
          } else {
              // only if any field can match, remove already used query keys
              // deleting a key is a very expensive operation so we don't do it
              // we build a new query instead with keys that were not indexed
              if(!matchAll) {
                  var newQuery = {};
                  for(var i = 0; i < keysNotIndexed.length; i++) {
                      newQuery[keysNotIndexed[i]] = query[keysNotIndexed[i]];
                  }
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + '\n=> to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
              docsRead++;
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  rec = merge(rec, data);
                  updated++;
                  JOURNAL.update(rec);
                  if(!multi) {
                      break;
                  }
              }
          }
          console.log('=> Updated docs', updated);
          if((!match || multi) && docsRead < TOTAL_DOCS) {
              console.log('Persistence - Loading Next Partition for Search. Flushing first...');
              JOURNAL.flush(); // flush before next paritition because current may have changes applied
              console.time('Persistence - Load Time');
              JOURNAL.load(function(err, collection, totalDocs, partitionIndex, partitionStart, partitionSize, partitionMax) {
                  // journal loads partition containing all records we can fit
                  if(collection)
                      if(collection.length + PERSISTENCE.COLLECTION.length < MAX_COLLECTION_SIZE)
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      else {
                          PERSISTENCE.COLLECTION.splice(0, collection.length);
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      }

                  console.timeEnd('Persistence - Load Time');
                  // NOTE: We receive these vars from JOURNAL each time we load a partition
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PERSISTENCE.COLLECTION.length;
                  PARTITION_MAX = partitionMax || 0;
                  //PARTITION_OFFSET = partitionStart || 0;
                  PARTITION_OFFSET = PERSISTENCE.COLLECTION[0]._blki || 0;
                  END_OFFSET = MAX_COLLECTION_SIZE - collection.length;
                  //PARTITION_OFFSET = END_OFFSET || 0;
                  console.log('Persistence - Total Docs in DB', TOTAL_DOCS + ' Partition Index', PARTITION  + ' Partition Start', PARTITION_OFFSET + ' LOADED', PARTITION_LOADED_SIZE + ' docs');

                  PERSISTENCE.updater(collection, query, data, multi, matchAll, docsRead);
              });
          }
      }
      console.log('Persistence - Updated', updated + ' documents');
      console.log('Persistence - Iterations:', i); // i gets hoisted to function scope
      return updated;
  }

  PERSISTENCE.remover = function(collection, query, multi, matchAll, docsR) {
      var docsRead = docsR || 0;
      var rec = {};
      var removed = 0;
      var match = false;
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      var blockIndices = []; // block indices to get from file that aren't in the collection range

      console.log('Persistence - Removing with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs && indexedRecs.length > 0) {
                  if(typeof(indexedRecs[0]) === 'object' && indexedRecs[0].length) { // assume 2D array
                      console.log('flattening index results array');
                      indexedRecs = [].concat.apply([], indexedRecs); // flatten the 2D array
                  }
                  console.log('=> Indexed Docs', indexedRecs.length);
                  //console.log(indexedRecs);
                  // we retrieve from memory first, then file, after index is done being read, and we have all the blki's
                  var blki = 0;
                  for(var i = indexedRecs.length-1; i >= 0; i--) {
                      blki = indexedRecs[i];
                      // NOTE We skip a loop cycle when the blki is not in loaded docs range so that we don't attempt to fetch it from memory.
                      // We then queue it up for file read after the index search is done
                      if(TOTAL_DOCS <= MAX_COLLECTION_SIZE) { // NOTE When whole collection is loaded
                          if(!(0 < blki == blki < collection.length)) {
                              //console.log('Not in range', blki);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1)
                                  blockIndices.push(blki);
                              continue; // next loop cycle
                          }
                      } else { // NOTE When whole collection is not loaded
                          if(!(PARTITION_OFFSET < blki == blki < PARTITION_OFFSET+collection.length)) {
                              //console.log('Not in range', blki, PARTITION_OFFSET, PARTITION_OFFSET+collection.length);
                              // if this block index is not already found, add it to list of blocks to load from file
                              if(blockIndices.indexOf(blki) === -1)
                                  blockIndices.push(blki);
                              continue; // next loop cycle
                          } else {
                              blki = blki - PARTITION_OFFSET;
                          }
                      }
                      // blki falls within currently loaded range
                      //console.log('In range', blki + ' Index', blki-PARTITION_OFFSET);
                      rec = collection[blki-PARTITION_OFFSET];

                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchAny(rec, query);
                      if(match) {
                          // check if we should update any index for this record
                          for(var p in INDEXER.INDICES) {
                              if(rec[p]) { // this field is indexed
                                  //console.log('UTIL.remover Updating indexed key', p);
                                  // Params: field, rec[field], data[field], block_index, deleting
                                  INDEXER.update(p, rec[p], '', rec._blki, true); // remove indices for any indexed fields in this record
                              }
                          }
                          JOURNAL.remove(rec._blki); // mark free block in journal
                          // We set each property to null, affecting the underlying memory, now we can't find it!
                          for(var p in rec) {
                              rec[p] = null;
                          }
                          removed++;
                          if(!multi) {
                              console.log('=> Removed One via index:', p);
                              break;
                          }
                      }
                  }
                  console.log('=> Removed docs', removed);
              } else {
                  // NOTE: This means not whole index is in memory, so we cannot exclude this field from normal search
                  console.log('=> Index does not contain anything for', p, query[p]);
                  keysNotIndexed.push(p);
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

      if(blockIndices.length > 0) // if we indexed docs in range outside of whats in memory
      {
          // NOTE Any unflushed changes cause finding inconsistent data in the partitions
          JOURNAL.flush();
          console.log('=> Getting from Journal', blockIndices.length + ' unique indexed docs (blkis) Removed', removed);
          // sort the block indices so we can get sequential access performance when reading from file
          UTIL.quickSort(blockIndices);
          // NOTE: Gets the records at the block indices, reading is a blocking operation
          indexedRecs = JOURNAL.loadBlki(blockIndices);
          // loop backwards due to INDEXER.update splicing stored values pointing to the record
          for(var j = indexedRecs.length-1; j >= 0; j--) {
              rec = indexedRecs[j];
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  JOURNAL.remove(rec._blki); // mark free block in journal
                  // check if we should update any index for this record
                  for(var p in INDEXER.INDICES) {
                      if(rec[p]) { // this field is indexed
                          //console.log('UTIL.remover Updating indexed key', p);
                          // Params: field, rec[field], data[field], block_index, deleting
                          INDEXER.update(p, rec[p], '', rec._blki, true); // remove indices for any indexed fields in this record
                      }
                  }
                  removed++;
                  if(!multi) {
                      console.log('=> Removed One via index:', p);
                      break;
                  }
              }
          }
          console.log('=> Removed docs', removed);
      }

      if(keysNotIndexed.length > 0) {
          if(!indexed) {
              console.log('=> No query field(s) are indexed!');
          } else {
              // only if any field can match, remove already used query keys
              // deleting a key is a very expensive operation from here so we don't do it
              // we build a new query instead with keys that were not indexed
              if(!matchAll) {
                  var newQuery = {};
                  for(var i = 0; i < keysNotIndexed.length; i++) {
                      newQuery[keysNotIndexed[i]] = query[keysNotIndexed[i]];
                  }
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + '\n=> to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
              docsRead++;
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  // Splice is ridiculously slow when we need to remove many records, so we don't use it
                  // Instead, we set each property value to null, now we can't find it in the collection!
                  // References pointing to this object are still valid but the data is null
                  // The indexer should have removed all references by the time we get here anyway
                  for(var p in rec) {
                      rec[p] = null;
                  }
                  removed++;
                  JOURNAL.remove(rec._blki);
                  if(!multi) {
                      break;
                  }
              }
          }
          console.log('=> Removed docs', removed);
          if((!match || multi) && docsRead < TOTAL_DOCS) {
              console.log('Persistence - Loading Next Partition for Search. Flushing first...');
              JOURNAL.flush(); // flush before next paritition because current may have changes applied
              console.time('Persistence - Load Time');
              JOURNAL.load(function(err, collection, totalDocs, partitionIndex, partitionStart, partitionSize, partitionMax) {
                  // journal loads partition containing all records we can fit
                  if(collection)
                      if(collection.length + PERSISTENCE.COLLECTION.length < MAX_COLLECTION_SIZE)
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      else {
                          PERSISTENCE.COLLECTION.splice(0, collection.length);
                          PERSISTENCE.COLLECTION = PERSISTENCE.COLLECTION.concat(collection);
                      }

                  console.timeEnd('Persistence - Load Time');
                  // NOTE: We receive these vars from JOURNAL each time we load a partition
                  TOTAL_DOCS = totalDocs || 0;
                  PARTITION = partitionIndex || 0;
                  PARTITION_LOADED_SIZE = partitionSize || 0;
                  PARTITION_CURRENT_SIZE = PERSISTENCE.COLLECTION.length;
                  PARTITION_MAX = partitionMax || 0;
                  //PARTITION_OFFSET = partitionStart || 0;
                  PARTITION_OFFSET = PERSISTENCE.COLLECTION[0]._blki || 0;
                  END_OFFSET = MAX_COLLECTION_SIZE - collection.length;
                  //PARTITION_OFFSET = END_OFFSET || 0;
                  console.log('Persistence - Total Docs in DB', TOTAL_DOCS + ' Partition Index', PARTITION  + ' Partition Start', PARTITION_OFFSET + ' LOADED', PARTITION_LOADED_SIZE + ' docs');

                  PERSISTENCE.remover(collection, query, multi, matchAll, docsRead);
              });
          }
      }
      console.log('Persistence - Removed', removed + ' documents');
      console.log('Persistence - Iterations:', i); // i gets hoisted to function scope
      return removed;
  }

  return PERSISTENCE;
}
