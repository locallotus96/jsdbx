'use strict';
var fs = require('fs');
var uuid = require('node-uuid');
var merge = require('merge');
var stream = require('stream');

var INDEXER = new(require('./INDEXER.js'));

var UTIL = {}; // utility object (class)

//--- UTILITIES

// Add the _id property to each object
UTIL.addIDProperty = function (obj) {
    if(obj.length) { // assuming an array
        for(var i = 0; i < obj.length; i++) {
            if(!obj[i]['_id'])
                obj[i]._id = uuid.v4().replace(/-/g, '');
        }
        return obj;
    } else { // single object
        if(!obj['_id'])
            obj._id = uuid.v4().replace(/-/g, '')
        return obj;
    }
}

UTIL.createIndex = function(field, collection) {
    if(field in INDEXER.INDECES) {
        return false; // index for this field exists
    } else {
        INDEXER.build(field, collection);
        return true;
    }
}

UTIL.destroyIndex = function (field) {
    return INDEXER.destroy(field);
}

/*
  data can be an object or an array of objects (1D or 2D array)
*/
UTIL.inserter = function(collection, data) {
    var inserted = 0;
    if(data.length) { // assuming an array
        var obj = {};
        for(var i = 0; i < data.length; i++) {
            obj = data[i];
            // check if new object contains a field to index on
            for(var p in INDEXER.INDECES) {
                // ok there's a field to index on
                if(p in obj) {
                    // index this record
                    INDEXER.add(p, obj);
                }
            }
            // check for [[obj,obj,],]
            if(obj.length > 0) { // array of objects hopefully
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
UTIL.finder = function(collection, query, multi, matchAll) {
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
                        match = this.matchAll(rec, query);
                    else
                        match = this.matchOne(rec, query);
                    if(match) {
                        // check if we've already found this object (multi reference issue)
                        //if(retDocs.indexOf(rec) >= 0) {
                        if(this.listContains(retDocs, rec)) {
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
                match = this.matchAll(rec, query);
            else
                match = this.matchOne(rec, query);
            if(match) {
                // check if we've already found this object (multi reference issue)
                //if(retDocs.indexOf(rec) >= 0) {
                if(this.listContains(retDocs, rec)) {
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

UTIL.remover = function(collection, query, multi, matchAll) {
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
                        match = this.matchAll(rec, query);
                    else
                        match = this.matchOne(rec, query);
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
                match = this.matchAll(rec, query);
            else
                match = this.matchOne(rec, query);
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

UTIL.updater = function(collection, query, data, multi, matchAll) {
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
                        match = this.matchAll(rec, query);
                    else
                        match = this.matchOne(rec, query);
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
                match = this.matchAll(rec, query);
            else
                match = this.matchOne(rec, query);
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

/*
    A helper function to return whether or not a document contains
    all fields and values in the query.
*/
UTIL.matchAll = function (rec, query) {
    for(var p in query) {
        /*if(!(p in rec) || !(rec[p] === query[p]))) {
            return false; // a field didn't match
        }*/
        // faster ~ 25%
        if(!(rec[p] === query[p])) {
            return false; // a field didn't match
        }
    }
    return true; // all fields match
}
// Like matchAll but for a single field (~ inverse)
UTIL.matchOne = function (rec, query) {
    for(var p in query) {
        /*if(p in rec && rec[p] === query[p]) {
            return true; // a field matches
        }*/
        // faster ~ 25%
        if(rec[p] === query[p]) {
            return true; // a field matches
        }
    }
    return false; // no fields match
}

UTIL.getObjectSize = function (obj) {
    return Object.keys(obj).length;
}

// Method to remove all nullified objects from a list
UTIL.filterDeleted = function (list) {
    for(var i = 0; i < list.length; i++) {
        if(list[i]._id === null) {
            list.splice(list[i], 1);
        }
    }
    return list;
}

// Method to return whether or not a list contains an object
UTIL.listContains = function (list, obj) {
    for(var i = 0; i < list.length; i++) {
        if(list[i] === obj) {
            return true;
        }
    }
    return false;
}

/*
  Is there some method of array like set in python?
  The standard way to do this is usually insert elements into a hash,
  then collect the keys - since keys are guaranteed to be unique.
  Or, similarly, but preserving order:
*/
// Slightly more consistent algorithm performance-wise
UTIL.getUniqueElements = function (collection) {
    var seen = {}; // set containing unique elements
    var result = [];
    var elem, elemStr;
    for (var i = 0; i < collection.length; i++) {
        elem = collection[i];
        // stringify the object it so it can be a valid unique property in the set
        elemStr = JSON.stringify(elem);
        if (!seen[elemStr]) {
            seen[elemStr] = true;
            result[result.length] = elem;
        }
    }
    return result;
}

// Removes duplicate elements from a collection / array
// Use the notion of a set, which every JavaScript object is an example of.
// Slightly faster on arrays with more unique elements compared to getUniqueElements()
// When not many unique elements are present, this is slower
// Slows down again when nearly all elements are unique, catch 22...
UTIL.deduplicate = function (collection) {
    var set = {};
    for(var i = 0; i < collection.length; i++) {
        // adds or replaces an entry in the set
        // stringify objects so they can be used as valid unique properties in the set
        // redundant for strings and numbers but still needed due to parse
        set[JSON.stringify(collection[i])] = true;
    }
    // Put all properties of the set back into the array
    collection = [];
    for(var elem in set) { // awlays gets the element as a string, so we need to parse it
        collection[collection.length] = JSON.parse(elem);
    }
    return collection;
}

/*
   Binary Search implementation for a sorted collection (array) of objects
   where each object contains a particular field that it's being indexed/sorted on, thus searched on
   The function takes an array of objects, and an object with a single key:value pair, the field/key must match the name
   and data type of the of the field each object is sorted on in the collection
   The function returns an array with the object found, and the index it was found at in the collection

   Params: collection:array[object, object..], searchKey:object{field:value}
   Returns: array[object, index]

   NOTE: The array to search through needs to be sorted
   Query aka Key is the field to search for, it assumes that the collection
   is sorted based on the field that this key represents
   ie: If the key is _id, the collection needs to be sorted by _id
*/
UTIL.binarySearch = function (collection, query) {
    var searchKey = Object.keys(query)[0];
    var searchVal = query[searchKey];
    var iMin = 0;
    var iMax = collection.length-1;
    var iMid;
    var iterations = 0;
    console.log(':: UTIL.binarySearch Collection:', collection.length + ' documents, Search Key/Val:', searchKey + ':', searchVal);
    while(iMin < iMax) {
        /*
        Selecting a pivot (middle) element is also complicated by the existence of integer overflow.
        If the boundary indices of the subarray being sorted are sufficiently large,
        the naïve expression for the middle index, (lo + hi)/2, will cause overflow and provide an invalid pivot index.
        This can be overcome by using, for example, lo + (hi−lo)/2 to index the middle element, at the cost of more complex arithmetic.
        */
        // calculate the midpoint for roughly equal partition
        //iMid = Math.floor((iMin + iMax) / 2);
        iMid = Math.floor(iMin + (iMax - iMin) / 2);
        iterations++;
        console.log(':: UTIL.binarySearch Checking Key:', collection[iMid][searchKey]);
        if(searchVal < collection[iMid][searchKey]) {
            iMax = iMid - 1;
        } else if(searchVal > collection[iMid][searchKey]) {
            iMin = iMid + 1;
        } else {
            console.log(':: UTIL.binarySearch Depth:', iterations + ', Found Node with Key/Val:', searchKey + ':', searchVal);
            return [collection[iMid], iMid]; // return the object at collection index iMid, and iMid
        }
    }
    console.log(':: UTIL.binarySearch Depth:', iterations + ', Could not find key');
    return [];
}

/*
    Used in quick-sort implementation in UTIL.partition()
    Used in selection-sort implementation in UTIL.selectionSort()
*/
UTIL.swap = function (collection, iOne, iTwo) {
    var temp = collection[iOne];
    collection[iOne] = collection[iTwo];
    collection[iTwo] = temp;
}
/*
    Used in quicksort implementation in UTIL.quickSort()
*/
UTIL.partition = function (collection, field, left, right) {
    /*
        Selecting a pivot (middle) element is also complicated by the existence of integer overflow.
        If the boundary indices of the subarray being sorted are sufficiently large,
        the naïve expression for the middle index, (lo + hi)/2, will cause overflow and provide an invalid pivot index.
        This can be overcome by using, for example, lo + (hi−lo)/2 to index the middle element, at the cost of more complex arithmetic.
    */
    //var pivot = collection[Math.floor((right + left) / 2)]; // middle index
    var pivot = collection[Math.floor(left + (right - left) / 2)][field]; // middle index
    var i = left; // starts from left and goes to pivot index
    var j = right; // starts from right and goes to pivot index
    // while the two indices don't match (not converged)
    while(i <= j) {
        while(collection[i][field] < pivot) {
            i++;
        }
        while(collection[j][field] > pivot) {
            j--;
        }
        // if the two indices still don't match, swap the values
        if(i <= j) {
            this.swap(collection, i, j);
            // change indices to continue loop
            i++;
            j--;
        }
    }
    // necessary for recursion
    return i;
}

/*
    Quicksort implementation.
    collection (array of objects) is sorted in place
    field (string) is the property of each object by which to sort on eg: '_id'
*/
// TODO: Does it still work?
UTIL.quickSort = function (collection, field, left, right) {
    var index;
    if(collection.length > 1) {
        // incase left and right aren't provided
        left = (typeof left != "number" ? 0 : left);
        right = (typeof right != "number" ? collection.length-1 : right);
        // split up the array
        index = this.partition(collection, field, left, right);
        if(left < index-1) {
            this.quickSort(collection, field, left, index-1);
        }
        if(index < right) {
            this.quickSort(collection, field, index, right);
        }
    }
    return collection;
}

/*
  Selection-sort implementation
  collection (array of objects) is sorted in place
  field (string) is the property of each object by which to sort on eg: '_id'

  Similar to bubble sort,it uses two loops to accomplish the task,
  ultimately resulting in the O(n2) complexity.
*/
// TODO: Does it still work?
UTIL.selectionSort = function (collection, field) {
    var len = collection.length;
    var min, i, j;
    for(i = 0; i < len; i++) {
        // set minimum to this position
        min = i;
        //check the rest of the array to see if anything is smaller
        for(j = i+1; j < len; j++) {
            if(collection[j][field] < collection[min][field]) {
                min = j;
            }
        }
        // if the minimum isn't in the position, swap it
        if(i != min) {
            this.swap(collection, i, min);
        }
    }
    return collection;
}

//--- ===================================================================
//--- FILE OPERATIONS ===================================================
//--- ===================================================================

UTIL.busyWriteStreaming = false; // are we currently streaming to the file?

UTIL.saveCollection = function (fd, collection, callback) {
    //this.saveFileStream(fd, collection, callback); // faster but we have to json stringify/parse the array and hit a size limit
    this.saveFileStreamLines(fd, collection, callback);
}

UTIL.loadCollection = function (fd, callback) {
    //this.loadFileStream(fd, callback); // faster but we have to json stringify/parse the array and hit a size limit
    this.loadFileStreamLines(fd, callback);
}

UTIL.saveFileStream = function (fd, collection, callback) {
    console.log('<=> Saving:', collection.length + ' records');
    if(collection.length === 0) {
        callback();
        return;
    }

    // streaming overwrites the file each new stream
    if(!this.busyWriteStreaming) {
        this.filterDeleted(collection);
        this.busyWriteStreaming = true;
        console.log('<=> Streaming. Old File Size:', this.getFilesizeInMBytes(fd));
        console.log('<=> Saving File:', fd);
        console.time('<=> Write File Stream Time');
        UTIL.streamToFile(fd, collection, function(err) {
            UTIL.busyWriteStreaming = false;
            console.log('<=> Write File Stream Error:', err + ' New File Size:', UTIL.getFilesizeInMBytes(fd));
            console.timeEnd('<=> Write File Stream Time');
            callback(err);
        });
    } else {
        callback(':: Busy write streaming!'); // signal error (we're just busy)
        return;
    }
}

UTIL.saveFileStreamLines = function (fd, collection, callback) {
    console.log('<=> Saving:', collection.length + ' records');
    if(collection.length === 0) {
        callback();
        return;
    }

    // streaming overwrites the file each new stream
    if(!this.busyWriteStreaming) {
        this.filterDeleted(collection);
        this.busyWriteStreaming = true;
        console.log('<=> Streaming lines. Old File Size:', this.getFilesizeInMBytes(fd));
        console.log('<=> Saving File:', fd);
        console.time('<=> Write File Stream Lines Time');
        UTIL.streamLinesToFile(fd, collection, function(err) {
            UTIL.busyWriteStreaming = false;
            console.log('<=> Write File Stream Lines Error:', err + ' New File Size:', UTIL.getFilesizeInMBytes(fd));
            console.timeEnd('<=> Write File Stream Lines Time');
            callback(err);
        });
    } else {
        callback(':: Busy write streaming!'); // signal error (we're just busy)
        return;
    }
}

UTIL.loadFileStream = function (fd, callback) {
    if(!this.isValidPathSync(fd) || !this.canReadWriteSync(fd)) {
        console.log(':: Error Opening File! Check File Name or Permissions...');
        callback(true);
    } else {
        console.log('<=> Streaming... File Size:', this.getFilesizeInMBytes(fd));
        console.log('<=> Loading File:', fd);
        console.time('<=> Read File Stream Time');
        this.streamFromFile(fd, function(err, data) {
            console.timeEnd('<=> Read File Stream Time');
            if(!err) {
                if(data.length > 0 && typeof(data) === 'object') {
                    console.log('<=> Loaded Collection:', data.length + ' records');
                    callback(null, data);
                } else {
                    callback(null, null); // no error but no data either, empty/new file
                }
            } else {
                console.log('<=> Read File Stream Error:', err + ' File Size:', UTIL.getFilesizeInMBytes(fd));
                callback(err, null);
            }
        });
    }
}

UTIL.loadFileStreamLines = function (fd, callback) {
    if(!this.isValidPathSync(fd) || !this.canReadWriteSync(fd)) {
        console.log(':: Error Opening File! Check File Name or Permissions...');
        callback(true);
    } else {
        console.log('<=> UTIL.loadCollection Streaming lines... File Size:', this.getFilesizeInMBytes(fd));
        console.log('<=> Loading File:', fd);
        console.time('<=> Read File Stream Lines Time');
        this.streamLinesFromFile(fd, function(err, data) {
            console.timeEnd('<=> Read File Stream Lines Time');
            if(!err) {
                if(data.length > 0 && typeof(data) === 'object') {
                    console.log('<=> Loaded Collection:', data.length + ' records');
                    callback(null, data);
                } else {
                    callback(null, null); // no error but no data either, empty/new file
                }
            } else {
                console.log('<=> Read File Stream Lines Error:', err + ' File Size:', UTIL.getFilesizeInMBytes(fd));
                callback(err, null);
            }
        });
    }
}

UTIL.streamToFile = function (fd, data, callback) {
    if(!data.length && typeof(data) === 'object') { // assuming a single object
        data = [data];
    }
    //--- Writable Stream
    var wstream = fs.createWriteStream(fd, {'flags': 'w', 'encoding': 'utf-8'});
    wstream.on('error', function(err) {
        console.error(':: Error writing to file stream!', err);
        callback(err); // signal error to callback
    });
    wstream.on('finish', function() {
        console.log('<=> Done writing to file stream!');
        callback(false); // signal done to callback with no error
    });
    wstream.write(JSON.stringify(data));
    wstream.end(); // emits 'finish' event
}

UTIL.streamLinesToFile = function (fd, data, callback) {
    if(!data.length && typeof(data) === 'object') { // assuming a single object
        data = [data];
    }
    //--- Writable Stream
    var wstream = fs.createWriteStream(fd, {'flags': 'w', 'encoding': 'utf-8'});
    var ok;
    wstream.on('error', function(err) {
        console.error(':: Error writing line to file stream!', err);
        callback(err); // signal error to callback
    });
    wstream.on('finish', function() {
        console.log('<=> Done writing lines to file stream!');
        callback(false); // signal done to callback with no error
    });
    var write = function () {
        for(var i = 0; i < data.length; i++) {
            ok = wstream.write(JSON.stringify(data[i]) + '\n');
            /*if(!ok) {
                // stops kernel memory buffer from flowing into userspace
                // which can cause a write or memory error
                // this happens because the writer can't keep up with the data coming in
                //console.log('Draining');
                wstream.once('Drain', write); // listener is not calling write again
                break;
            }*/
        }
    }
    write();
    //data.forEach(function(obj) { // no real performance difference
    //    wstream.write(JSON.stringify(obj) + '\n');
    //});
    wstream.end(); // emits 'finish' event
}

// Reads whole file into data object,
// that's why we hit a json length limit since everything is one giant json string object
// javascript can't serialize like other languages...
UTIL.streamFromFile = function (fd, callback) {
    //--- Readable Stream
    var rstream = fs.createReadStream(fd, {'encoding': 'utf-8'});
    //rstream.setEncoding('utf8');
    var data = '';
    rstream.on('error', function(err) {
        console.error(':: Error reading from file stream!', err);
        callback(err, null);
    });
    rstream.on('end', function() {
        console.log('<=> Done reading from file stream!');
        if(data) {
            callback(null, JSON.parse(data));
        } else {
            callback(null, {});
        }
    });
    rstream.on('data', function(chunk) {
        data += chunk;
    });
}

// Read and parse lines from the file,
// individual lines aka objects should be json stringified upon insertion
UTIL.streamLinesFromFile = function (fd, callback) {
    // Read lines via stream (slower)
    var data = [];
    var rl = require('readline').createInterface({
        input: fs.createReadStream(fd, {'encoding': 'utf-8'})
    });

    rl.on('line', function (line) {
        //console.log('Line from file:\n', line);
        //callback(null, JSON.parse(line));
        data[data.length] = JSON.parse(line);
    });

    rl.on('close', function () {
        console.log('<=> Done streaming lines - closing file');
        callback(null, data);
    });
}

// Same performance as streamLinesToFile
/*UTIL.streamLinesFromFile2 = function (fd, callback) {
    var data = [];
    //--- Writable Stream
    lr = new(require('line-by-line'));
    lr.on('error', function(err) {
        console.error(':: Error reading line from file!', err);
        callback(err); // signal error to callback
    });
    lr.on('line', function (line) {
        // 'line' contains the current line without the trailing newline character.
        data[data.length] = JSON.parse(line);
    });
    lr.on('end', function () {
        // All lines are read, file is closed now.
        console.log(':: Done reading lines - closing file');
        callback(null, data);
    });
}*/

UTIL.readFromFileSync = function (fd) {
    return fs.readFileSync(fd, 'utf-8');
}

UTIL.readFromFileAsync = function (fd, callback) {
    fs.readFile(fd, 'utf-8', function(err, data) {
        if(err) {
            console.error(':: Error Reading from File!', err);
            throw err;
            callback(err);
        } else {
            callback(null, data);
        }
    });
}

UTIL.appendToFileAsync = function (fd, data, callback) {
    var buffer = [];
    if(data.length) {
        buffer = data.slice();
    } else {
        buffer.push(data);
    }
    this.appendLineAsync(buffer, null, fd, callback);
}

// helper function for appendToFileAsync()
// appends one line at a time
UTIL.appendLineAsync = function (buffer, error, fd, callback) {
    // finished when buffer is empty
    if(buffer.length === 0) {
        callback(err);
        return;
    }
    var data = buffer.shift();
    fs.appendFile(fd, JSON.stringify(data) + '\n', 'utf8', function(err) { // default encodes to utf8
        if(err) {
            console.error(':: Error Appending to File!', err);
            throw err;
        } else {
            UTIL.appendLineAsync(buffer, err, fd, callback);
        }
    });
}

UTIL.appendToFileSync = function (fd, data) {
    for(var i = 0; i < data.length; i++) {
        fs.appendFileSync(fd, JSON.stringify(data[i]) + '\n', {encoding: 'utf8', flag: 'a'});
    }
}

UTIL.removeFileSync = function (fd) {
    if(fs.unlinkSync(fd)) {
        return true;
    } else {
        return false;
    }
}

UTIL.removeFileAsync = function (fd, callback) {
    fs.unlink(fd, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

// check if file is visible to calling process
UTIL.isValidPathSync = function (path) {
    try {
        fs.accessSync(path, fs.F_OK);
    } catch (err) {
        return false;
    }
    return true;
}

// check if file is visible to calling process
UTIL.isValidPathAsync = function (path, callback) {
    fs.access(path, fs.F_OK, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

// abstraction for exposed function
UTIL.isValidPath = function (path) {
    return this.isValidPathSync(path);
}


// check read and write permissions
UTIL.canReadWriteSync = function (path) {
      try {
          fs.accessSync(path, fs.R_OK | fs.W_OK);
      } catch (err) {
          return false;
      }
      return true;
}

// check read and write permissions
UTIL.canReadWriteAsync = function (path, callback) {
    fs.access(path, fs.R_OK | fs.W_OK, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

UTIL.resetFileSync = function (fd) {
    //fs.writeFileSync(fd, '', 'utf8', {encoding: 'utf8', flag: 'a');
    fs.writeFile(fd, '', 'utf8', function(err) {
        if(err) {
            console.error(err);
            throw err;
        }
    });
}

UTIL.getFilesizeInBytes = function (fd) {
    try {
      var stats = fs.statSync(fd);
      return stats['size'];
    } catch (err) {
        return false;
    }
}

UTIL.getFilesizeInMBytes = function (fd) {
    try {
        var stats = fs.statSync(fd);
        return (stats['size'] / 1024.0) / 1024.0;
    } catch (err) {
        return false;
    }
}

// Allows using new UTIL()
module.exports = function() {
    return UTIL;
}
//module.exports = UTIL;
