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
              for(var p in INDEXER.INDECES) // check if new object contains a field to index on
                  if(p in obj) // ok there's a field to index on
                      INDEXER.add(p, obj); // index this record
          }
      } else { // single object
          if(!obj['_id'])
              obj._id = uuid.v4().replace(/-/g, '')
          for(var p in INDEXER.INDECES) // check if new object contains a field to index on
              if(p in obj) // ok there's a field to index on
                  INDEXER.add(p, obj); // index this record
      }
      return obj;
  }

  PERSISTENCE.createIndex = function(field, collection) {
      if(field in INDEXER.INDECES) {
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
      Indexed searches are faster by roughly a factor of O(O / numIndexedFields), when doing a multi search
      on one or more indexed and none indexed fields.
  */
  PERSISTENCE.finder = function(collection, query, multi, matchAll) {
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
          if(p in INDEXER.INDECES) { // this field is indexed
              indexed = true;
              console.log('=> Query is indexed via', p);
              indexedRecs = INDEXER.get(p, query[p]);
              if(indexedRecs) {
                  for(var i = 0; i < indexedRecs.length; i++) {
                      rec = indexedRecs[i];
                      if(matchAll)
                          match = UTIL.matchAll(rec, query);
                      else
                          match = UTIL.matchOne(rec, query);
                      if(match) {
                          // check if we've already found this object (multi reference issue)
                          //if(retDocs.indexOf(rec) >= 0) {
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
                  match = UTIL.matchOne(rec, query);
              if(match) {
                  // check if we've already found this object (multi reference issue)
                  //if(retDocs.indexOf(rec) >= 0) {
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
          if(p in INDEXER.INDECES) { // this field is indexed
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
                          match = UTIL.matchOne(rec, query);
                      if(match) {
                          // check if we should update any index for this record
                          for(var p in rec) {
                              if(p in INDEXER.INDECES) { // this field is indexed
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
                  match = UTIL.matchOne(rec, query);
              if(match) {
                  // console.log('UTIL.remover Splicing array index:', i);
                  // splice also mutates the array that calls it.
                  // and we throw away the new array because we're removing all those records
                  // console.log(collection.splice(i, 1));

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
          if(p in INDEXER.INDECES) { // this field is indexed
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
                          match = UTIL.matchOne(rec, query);
                      if(match) {
                          // check if we should update any index for this record
                          for(var p in rec) {
                              if(p in INDEXER.INDECES) { // this field is indexed and changing
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
                  match = UTIL.matchOne(rec, query);
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
