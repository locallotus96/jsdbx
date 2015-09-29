'use strict';
var fs = require('fs');
var stream = require('stream');

// TODO: Move All file operations to own class - FILER

//-- UTILITIES - Helper Functions

var UTIL = {}; // utility object (class)

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

UTIL.saveFileStream = function (fd, collection, callback) {
    if(collection.length === 0) {
        callback();
        return;
    }
    // streaming overwrites the file each new stream
    if(!this.busyWriteStreaming) {
        console.time('<=> Filter Null Records Time');
        this.filterDeleted(collection);
        console.timeEnd('<=> Filter Null Records Time');
        this.busyWriteStreaming = true;
        console.log('<=> Streaming to:', fd + ' Old File Size:', this.getFilesizeInMBytes(fd));
        console.time('<=> Write File Stream Time');
        this.streamLinesToFile(fd, collection, function(err) {
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

UTIL.loadFileStream = function (fd, callback) {
    if(!this.isValidPathSync(fd) || !this.canReadWriteSync(fd)) {
        console.log(':: Error Opening File! Check File Name or Permissions...');
        callback(true);
    } else {
        console.log('<=> Streaming from:', fd + ' File Size:', this.getFilesizeInMBytes(fd));
        console.time('<=> Read File Stream Time');
        this.streamFromFile3(fd, function(err, data) {
            console.timeEnd('<=> Read File Stream Time');
            if(!err) {
                if(data.length > 0 && typeof(data) === 'object') {
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
    wstream.write(JSON.stringify(data)); // warning!!!
    wstream.end(); // emits 'finish' event
}

// was very slow, ~30sec for 1mil rec, now ~2sec with writing "buffered" strings of lines
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
    var sout = '';
    var lCount = 0;
    var lMax = 1024 < data.length ? 1024 : data.length;
    console.log('<=> Max lines to buffer', lMax);
    var write = function () {
        for(var i = 0; i < data.length; i++) {
            sout += JSON.stringify(data[i]) + '\n';
            lCount++;
            if(lCount === lMax || i === data.length-1) {
                //ok = wstream.write(JSON.stringify(data[i])) + '\n');
                ok = wstream.write(sout);
                sout = '';
                lCount = 0;
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
    }
    write();
    wstream.end(); // emits 'finish' event
}

// Reads and parse chunks from file as string, then parse the whole string
UTIL.streamFromFile = function (fd, callback) {
    //--- Readable Stream, read whole file very fast
    var data = '';
    var rs = fs.createReadStream(fd, {'encoding': 'utf-8', 'bufferSize': 64 * 1024});
    rs.on('error', function(err) {
        console.error(':: Error reading from file stream!', err);
        callback(err, null);
    });
    rs.on('data', function(chunk) {
        data += chunk;
    });
    rs.on('end', function() {
        console.log('<=> Done reading from file stream!');
        if(data) {
            callback(null, JSON.parse(data)); // warning!!!
        } else {
            callback(null, {});
        }
    });
}

UTIL.streamFromFile2 = function (fd, callback) {
    var data = [];
    var l, len, prev = '';
    var CHUNK_SIZE = 128 * 1024;
    // The node implementation forces the buffer size to 64*1024 (65536) bytes
    // You can even configure the initial size of the buffer by passing: highWatermark: CHUNK_SIZE
    var rs = fs.createReadStream(fd, {'encoding': 'utf-8', highWatermark: CHUNK_SIZE});
    rs.on('error', function(err) {
        console.error(':: Error reading from file stream!', err);
        callback(err, null);
    });
    // This forces the limitation of reading with the internal node buffer size
    rs.on('data', function(chunk) {
        len = chunk.length;
        //console.log(len);
        l = (prev + chunk).split('\n');
        prev = len === chunk.length ? '\n' + l.splice(l.length-1)[0] : '';
        l.forEach(function(line) {
            if(!line)
                return;
            data[data.length] = JSON.parse(line);
        });
    });
    rs.on('end', function() {
        console.log('<=> Done reading from file stream!');
        if(data) {
            callback(null, data);
        } else {
            callback(null, {});
        }
    });
}

// Read and parse chunks from file into lines, but much slower, strill faster than streamLines tho
UTIL.streamFromFile3 = function (fd, callback) {
    var data = [], chunk;
    var l, len, prev = '';
    var CHUNK_SIZE = 128 * 1024;
    // The node implementation forces the buffer size to 64*1024 (65536) bytes
    // You can even configure the initial size of the buffer by passing: highWatermark: CHUNK_SIZE
    var rs = fs.createReadStream(fd, {'encoding': 'utf-8', highWatermark: CHUNK_SIZE});
    rs.on('error', function(err) {
        console.error(':: Error reading from file stream!', err);
        callback(err, null);
    });
    // Here we get around the internal buffer limit
    // If CHUNK_SIZE is larger than the internal buffer,
    // node will return null and buffer some more before emitting readable again.
    rs.on('readable', function() {
        while(null !== (chunk = rs.read(CHUNK_SIZE))) {
            len = chunk.length;
            //console.log(len);
            l = (prev + chunk).split('\n');
            prev = len === chunk.length ? '\n' + l.splice(l.length-1)[0] : '';
            l.forEach(function(line) {
                if(!line)
                    return;
                data[data.length] = JSON.parse(line);
            });
        }
    });
    rs.on('end', function() {
        console.log('<=> Done reading from file stream!');
        if(data) {
            callback(null, data);
        } else {
            callback(null, {});
        }
    });
}

// Read and parse lines from the file,
// individual lines aka json objects should be stringified upon insertion
// slightly slower than chunked streamFromFile2-3
UTIL.streamLinesFromFile = function (fd, callback) {
    var data = [];
    var rsl = require('readline').createInterface({
        input: fs.createReadStream(fd, {'encoding': 'utf-8'})
    });
    rsl.on('line', function (line) {
        //console.log('Line from file:\n', line);
        //callback(null, JSON.parse(line));
        data[data.length] = JSON.parse(line);
    });
    rsl.on('close', function () {
        console.log('<=> Done streaming lines from file!');
        callback(null, data);
    });
}

// Same performance as streamLinesFromFile without "buffer"
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

// ~ Same performance as streamLinesToFile
UTIL.bufferWriteFileSync = function (fd, data, callback) {
    if(!data.length && typeof(data) === 'object') { // assuming a single object
        data = [data];
    }
    var o = fs.openSync(fd, 'w');
    var sout = '', bout;
    var lCount = 0;
    var lMax = 1024 < data.length ? 1024 : data.length;
    console.log('<=> Max lines to buffer', lMax);
    for(var i = 0; i < data.length; i++) {
        sout += JSON.stringify(data[i]) + '\n';
        lCount++;
        if(lCount === lMax || i === data.length-1) {
            bout = new Buffer(sout, 'utf8');
            //console.log('<<<< Writing buffer of length', bout.length + ' bytes');
            fs.writeSync(o, bout, 0, bout.length);
            sout = '';
            lCount = 0;
        }
    }
    fs.closeSync(o);
    console.log('<=> Done buffer writing to file!');
    callback(false);
}

// ~ Same performance as streamFromFile3
UTIL.bufferReadFileSync = function (fd, callback) {
    var i = fs.openSync(fd, 'r');
    var bin = new Buffer(1024 * 1024);
    var l, len, prev = '';
    var data = [];
    while(len = fs.readSync(i, bin, 0, bin.length)) {
        l = (prev + bin.toString('utf8', 0, len)).split('\n');
        prev = len === bin.length ? '\n' + l.splice(l.length-1)[0] : '';
        l.forEach(function(line) {
            if(!line)
                return;
            data[data.length] = JSON.parse(line);
        });
    }
    fs.closeSync(i);
    console.log('<=> Done buffer reading from file!');
    callback(null, data);
}

UTIL.readFromFileSync = function (fd) {
    return fs.readFileSync(fd, 'utf-8');
}

UTIL.readFromFileAsync = function (fd, callback) {
    fs.readFile(fd, 'utf-8', function(err, data) {
        if(err) {
            console.error(':: Error Reading from File!', err);
            callback(err);
        } else {
            callback(null, data);
        }
    });
}

// stupid function, copies data in memory
UTIL.appendToFileAsync = function (fd, data, callback) {
    var buffer = [];
    if(data.length) { // get object(s) from array
        buffer = data.slice(); // copies the data
    } else { // single object was given
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
    var data = buffer.shift(); // shift out first element
    fs.appendFile(fd, JSON.stringify(data) + '\n', 'utf8', function(err) { // default encodes to utf8
        if(err) {
            console.error(':: Error Appending to File!', err);
            throw err;
        } else { // loop this function until buffer is empty
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

module.exports = UTIL;
