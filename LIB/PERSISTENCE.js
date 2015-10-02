'use strict';
// Global Modules
var uuid = require('node-uuid');
var merge = require('merge');
// Local Modules
var UTIL = require('./UTIL.js');
var FILER = require('./FILER.js');

//--- PERSISTENCE LAYER

module.exports = function() {
  var PERSISTENCE = {};
  var INDEXER = new require('./INDEXER.js')();

  PERSISTENCE.saveCollection = function (fd, collection, callback) {
      console.log('=> Saving - Filtering null records...');
      console.time('<=> Filter Null Records Time');
      UTIL.filterDeleted(collection);
      console.timeEnd('<=> Filter Null Records Time');
      console.log('=> Saving:', collection.length + ' records...');
      FILER.saveFileStream(fd, collection, callback);
  }

  PERSISTENCE.loadCollection = function (fd, callback) {
      FILER.loadFileStream(fd, callback);
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
              obj._id = uuid.v4().replace(/-/g, '')
          for(var p in INDEXER.INDICES) // check if new object contains a field to index on
              if(obj[p]) // ok there's a field to index on
                  INDEXER.add(p, obj); // index this record
      }
      return obj;
  }

  PERSISTENCE.createIndex = function(field, collection) {
      if(INDEXER.INDICES[field]) {
          return false; // index for this field exists
      } else {
          INDEXER.build(field, collection);
          return true;
      }
  }

  PERSISTENCE.destroyIndex = function (field) {
      return INDEXER.destroy(field);
  }

  /*
    data can be an object or an array of objects (1D or 2D array)
  */
  PERSISTENCE.inserter = function(collection, data) {
      var inserted = 0;
      if(data.length) { // assuming an array
          var obj = {};
          for(var i = 0; i < data.length; i++) {
              obj = data[i];
              // check for [[obj,obj,],]
              if(obj.length && typeof obj === 'object') { // array of objects hopefully
                  collection.concat(this.addIDProperty(obj));
                  inserted += obj.length;
              } else if(typeof obj === 'object') { // single object
                  collection.push(this.addIDProperty(obj));
                  inserted++;
              } else {
                  // invalid data encountered
                  console.error(':: DAL.insert Error in record(s) to insert!');
              }
          }
      } else { // single object
          collection.push(this.addIDProperty(data));
          inserted++;
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
  PERSISTENCE.finder = function(collection, query, multi, matchAll, options) {
      var retDocs = [];
      var rec = {};
      var match = false; // whether or not a record matches the query
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      console.log('UTIL.finder Finding with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs) {
                  for(var i = 0; i < indexedRecs.length; i++) {
                      rec = indexedRecs[i];
                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchAny(rec, query);
                      if(match) {
                          // check if we've already found this object (multi index reference issue)
                          if(UTIL.listContains(retDocs, rec)) {
                              //console.log('UTIL.finder Already found!');
                              continue; // next loop cycle
                          }
                          retDocs.push(rec);
                          if(!multi) {
                              console.log('UTIL.finder Found One via index:', p);
                              break; //return this.getUniqueElements(retDocs); // slower
                          }
                      }
                  }
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

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
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + ' to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  // check if we've already found this object (multi index reference issue)
                  if(UTIL.listContains(retDocs, rec)) {
                      //console.log('UTIL.finder Already found!!');
                      continue; // next loop cycle
                  }
                  retDocs.push(rec);
                  if(!multi) {
                      break;
                  }
              }
          }
      }
      if(options && retDocs.length > 0) {
          // if we have a selection query, return only those fields from matching docs
          // TODO: Try modifying array in place by removing fields from each rec
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
                      // TODO: find avg fastest sort algorithm
                      // quick sort the array in place
                      if(typeof(sort) === 'object') { // check for sort:{field:order}
                          console.log('=> Sorting on', sort);
                          UTIL.quickSort(retDocs, sort); // quickSort is ascending
                          if(sort[Object.keys(sort)[0].toString()] === -1) { // we want descending
                              retDocs.reverse(); // // reverse the array in place, very fast in v8
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
              }
          }
      }
      console.log('UTIL.finder Found', retDocs.length + ' documents in', i + ' iterations');
      return retDocs; //this.getUniqueElements(retDocs); // slower
  }

  PERSISTENCE.remover = function(collection, query, multi, matchAll) {
      var rec = {};
      var removed = 0;
      var match = false;
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      console.log('UTIL.remover Removing with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs) {
                  // loop backwards due to INDEXER.update splicing stored values pointing to the record
                  for(var j = indexedRecs.length-1; j >= 0; j--) {
                      rec = indexedRecs[j];
                      if(!rec) continue;

                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchAny(rec, query);
                      if(match) {
                          // check if we should update any index for this record
                          for(var p in INDEXER.INDICES) {
                              if(rec[p]) { // this field is indexed
                                  //console.log('UTIL.remover Updating indexed key', p);
                                  INDEXER.update(p, rec[p], '', rec, true); // remove indices for any indexed fields in this record
                              }
                          }
                          // We set each property to null, affecting the underlying memory, now we can't find it!
                          for(var p in rec) {
                              rec[p] = null;
                          }
                          removed++;
                          if(!multi) {
                              break;
                          }
                      }
                  }
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

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
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + ' to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
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
                  if(!multi) {
                      break;
                  }
              }
          }
      }
      console.log('UTIL.remover Removed', removed + ' documents');
      console.log('UTIL.remover Iterations:', i); // i gets hoisted to function scope
      return removed;
  }

  PERSISTENCE.updater = function(collection, query, data, multi, matchAll) {
      var rec = {};
      var updated = 0;
      var match = false;
      var indexed = false; // are any keys in the query indexed?
      var keysNotIndexed = []; // all search keys that were not found in the index
      var indexedRecs = []; // holding records returned from indexer
      console.log('UTIL.updater Finding with Query:', JSON.stringify(query) + ' Multi:', multi + ' MatchAllFields:', matchAll);
      console.log('UTIL.updater Updating with Data:', JSON.stringify(data));

      // INDEX SEARCH
      // check if we have an index for this search query
      for(var p in query) {
          if(INDEXER.INDICES[p]) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs) {
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
                              if(rec[p]) { // this field is indexed and changing
                                  //console.log('UTIL.updater Updating indexed key', p);
                                  INDEXER.update(p, rec[p], data[p], rec, false);
                              }
                          }
                          rec = merge(rec, data);
                          updated++;
                          if(!multi) {
                              break;
                          }
                      }
                  }
              }
          } else {
              keysNotIndexed.push(p);
          }
      }
      // END INDEX SEARCH

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
                  console.log('=> Rebuilt Query from', JSON.stringify(query) + ' to', JSON.stringify(newQuery));
                  query = newQuery;
              }
          }
          for(var i = 0; i < collection.length; i++) {
              rec = collection[i];
              if(matchAll)
                  match = UTIL.matchAll(rec, query);
              else
                  match = UTIL.matchAny(rec, query);
              if(match) {
                  collection[i] = merge(rec, data);
                  updated++;
                  if(!multi) {
                      break;
                  }
              }
          }
      }
      console.log('UTIL.updater Updated', updated + ' documents');
      console.log('UTIL.updater Iterations:', i); // i gets hoisted to function scope
      return updated;
  }

  return PERSISTENCE;
}
