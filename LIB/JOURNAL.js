'use strict';
//var FILER = require('./FILER.js');
var UTIL = require('./UTIL.js');

//=====================================================

module.exports = function(fd, STORE) {
    var FILE = fd.substring(0, fd.length-2) + 'journal';
    console.log('Journal File:', FILE);

    // Block level storage module / Chunk Store or MMAP
    //STORE = new require('./STORE.js')(fd); // Chunk Store
    var STORE = new require('./MMAP.js')(fd); // MMAP

    // CACHING, 512 MB
    var lruCache = require('lru-cache');
    var options = { max: 128000 // 512 MB
                , maxAge: 1000 * 60 * 60 }; // 60 minutes
    var CACHE = lruCache(options);

    // TODO: Implement own queue with built in size
    // FIFO queue in Javascript - implemented using a double linked-list
    // Get new FIFO queue object
    var iQ = require('fifo')(); // Journal Queue for inserts
    var uQ = require('fifo')(); // Journal Queue for updates
    //var fQ = require('fifo')(); // Queue for free blocks (deleted docs)
    // NOTE: Deleted docs corresponding block indices are marked for overwrite by subsequent inserts

    var LAST_FLUSH = new Date();

    var DOC_SIZE = STORE.DOC_SIZE;
    var PARTITION_INDEX = 0; // what block no. of x GB is in memory? Increments on load
    var PARTITION_START = 0; // what doc offet are we loading from?
    var PARTITION_MAX = STORE.MAP_SIZE / DOC_SIZE; // x amount of records to hold in memory: 256000 * DOC_SIZE = 1 GB
    var PARTITION_SIZE = PARTITION_MAX; // grows/shrinks as file grows up to max 1GB before we start swapping

    var J = {}; // returned object via exports
    // flush changes in journal to db every n seconds
    J.FLUSH_INTERVAL = 30; // config var, seconds
    // max number of journal entries to queue up before flushing to db
    J.MAX_QUEUE = 1024; // config var, max docs in each queue before saving to disk for inserts and updates

    // block indices marked by delete, gets used for overwrite under certain insert criteria..
    // TODO: replace array with queue?
    J.FREE_BLOCKS = [];
    J.REUSED_BLOCKS = 0; // counter of blocks to inserted into free space
    J.PARTITION_MAX = PARTITION_MAX;

    J.getTotalDocs = function() {
        return (STORE.getTotalSize() / DOC_SIZE);
    }

    J.getNoPartitions = function() {
        return Math.ceil(this.getTotalDocs() / PARTITION_MAX);
    }

    J.insert = function(rec) {
        if(rec.length) {
            console.log('Journal - Batch Insert - Immediate Write', rec.length + ' docs');
            this.write(rec); // we say batch inserts are applied immediately
        }
        else {
            //this.write([rec]);
            CACHE.set(rec._blki, rec);
            iQ.push(rec);
        }
        this.iCheck();
    }

    J.update = function(rec) {
        CACHE.set(rec._blki, rec);
        uQ.push(rec);
        this.iCheck();
    }

    J.remove = function(i) { // i = blki = block index
        // Mark deleted doc block index as available for new writes
        this.FREE_BLOCKS.push(i);
    }

    // interval checks
    J.iCheck = function() {
        console.log('Journal - Flush Interval', this.FLUSH_INTERVAL*1000, 'Time Since Last Flush', new Date() - LAST_FLUSH);
        console.log('Journal - Free Blocks', this.FREE_BLOCKS.length, 'Insert Queue', iQ.length, 'Update Queue', uQ.length);
        if(new Date() - LAST_FLUSH > this.FLUSH_INTERVAL*1000 || iQ.length >= this.MAX_QUEUE) {
            this.flushIQueue(); // flush insert queue to db
        }
        if(new Date() - LAST_FLUSH > this.FLUSH_INTERVAL*1000 || uQ.length >= this.MAX_QUEUE) {
            this.flushUQueue(); // flush update queue to db
        }
    }

    // Function not used
    J.write = function(data) {
        STORE.write(data, function(err){
            //console.log('Journal - Critical Write Error:', err);
        });
    }

    J.flushIQueue = function() {
        //console.log('Journal - Flushing Insert Queue to DB', iQ_COUNT + ' docs');
        //if(iQ.isEmpty())
            //console.log('Journal - Insert Queue is empty');
        if(!(iQ.isEmpty())) {
            var docs = UTIL.quickSort(iQ.toArray(), '_blki');
            STORE.write(docs, function(err){
                //console.log('Journal - Critical Write Error:', err);
            }, false, this.REUSED_BLOCKS > 0 ? this.REUSED_BLOCKS : null); // update = false, fill block space = ? true : false
            iQ.removeAll();
            if(docs <= this.REUSED_BLOCKS)
                this.REUSED_BLOCKS = 0; // reset since we'll be using all free blocks
        }
    }

    J.flushUQueue = function() {
        //console.log('Journal - Flushing Update Queue to DB', uQ_COUNT + ' docs');
        //if(uQ.isEmpty())
            //console.log('Journal - Update Queue is empty');
        if(!(uQ.isEmpty())) {
            // sort the block indices so we can have 'sequential' file access
            STORE.write(UTIL.quickSort(uQ.toArray(), '_blki'), function(err){
                //console.log('Journal - Critical Write Error:', err);
            }, true); // true to signal update
            uQ.removeAll();
        }
    }

    J.flush = function() { // flush queued changes to db
        console.log('Journal - Flushing queued docs to DB: Inserts', iQ.length, 'Updates', uQ.length, 'Free Blocks', this.FREE_BLOCKS.length); // save everything we mapped up untill new additions to the collection (we save these as they arrive)
        console.time('Journal - Flush Time');
        this.flushIQueue(); // flush insert queue to db
        this.flushUQueue(); // flush update queue to db
        console.timeEnd('Journal - Flush Time');
    }

    J.load = function(callback) {
        var fileSize = STORE.getFileSize(PARTITION_INDEX); //FILER.getFilesizeInBytes(fd);
        var docsInFile = fileSize / DOC_SIZE;
        var totalSize = STORE.getTotalSize();
        var totalDocs = totalSize / DOC_SIZE;
        if(fileSize < DOC_SIZE) { // DOC_SIZE = doc block / chunk size in file
            console.log('Journal - Nothing to load...');
            callback(false, null, 0, 0, 0); // not an error, just 'empty' file
            return;
        } else if(docsInFile < PARTITION_MAX) { // 1 GB default, DOC_SIZE = doc block / chunk size in file
            console.log('Journal - File size is smaller than expected partition size - Adjusting Partition Size...');
            PARTITION_SIZE = fileSize / DOC_SIZE; // set the parition size to that of no. docs we will hold in memory
        } else if(docsInFile < (PARTITION_SIZE + PARTITION_START) / DOC_SIZE){
            console.log('Journal - No more partitions to load...');
            //callback(false); // not an error, already loaded last partition
            J.reload(callback); // start again
            return;
        }
        if(totalDocs <= PARTITION_START){ // we are starting from first partition again
            console.log('Journal - Cycling...');
            PARTITION_START = 0;
            PARTITION_INDEX = 0;
            PARTITION_SIZE = PARTITION_MAX;
        } //else if((PARTITION_SIZE*DOC_SIZE) + (PARTITION_START*DOC_SIZE) > fileSize) {
            //console.log('Journal - File size smaller than expected partition size, adjusting...');
            //PARTITION_SIZE = (fileSize / DOC_SIZE) - PARTITION_START;
        //}
        //PARTITION_SIZE = (fileSize / DOC_SIZE) - ((PARTITION_INDEX+1) * PARTITION_MAX);
        //console.log('Journal - File Size:', fileSize + ' Stored Docs:', fileSize / DOC_SIZE);
        console.log('Journal - Load Start:', DOC_SIZE * PARTITION_START + ' BLKI', PARTITION_START + ' Partition Index:', PARTITION_INDEX );
        console.log('Journal - Expected Load Size:', DOC_SIZE * PARTITION_SIZE, 'Docs', PARTITION_SIZE);
        console.log('Journal - Expected End:', (DOC_SIZE * PARTITION_SIZE) + (PARTITION_START * DOC_SIZE), 'BLKI', PARTITION_SIZE + PARTITION_START - 1);
        PARTITION_START += PARTITION_SIZE;
        PARTITION_INDEX++;
        //console.log('Journal - Next Expected Load Start:', PARTITION_START + ' Parition Index:', PARTITION_INDEX);

        var cached = []; // cached docs
        var uncached = []; // unached block indices
        // var cachedObj = {};
        /*for(var i = PARTITION_START-PARTITION_SIZE; i < (PARTITION_START-PARTITION_SIZE)+PARTITION_SIZE; i++) {
              cachedObj = CACHE.get(i);
              if(cachedObj) // i = block index
                  cached.push(cachedObj);
              else
                  uncached.push(i);
        }*/

        if(uncached.length > 0/*uncached.length !== PARTITION_SIZE*/) {
            // NOTE When we are trying to get a number of blocks
            uncached = this.loadBlki(uncached);
            console.log('Journal - Got Uncached', uncached.length);
            console.log('Journal - Got Cached', cached.length);
            cached = cached.concat(uncached); // load uncached blocks, will be automatically cached by loadBlki() after retrieval
            // we have to send back a copy of cached otherwise we lose reference to all docs in collection on next load
            callback(null, cached, (totalSize / DOC_SIZE), PARTITION_INDEX-1, PARTITION_START-PARTITION_SIZE, PARTITION_SIZE, PARTITION_MAX); // no error, pass data
        } else {
            // NOTE when we're trying to iterate over large contagious blocks or the entire db, such as when building the indices, faster than many individual block reads
            STORE.load(PARTITION_START-PARTITION_SIZE, PARTITION_SIZE, function(err, loaded) { // 1 GB of records
                if(!err) {
                    // NOTE: Thrashing the cache and looping results slows us down a shit tonne!
                    /*if(loaded) {
                        for(var i = 0; i < loaded.length; i++)
                            if(!CACHE.peek(loaded[i]._blki))
                                CACHE.set(loaded[i]._blki, loaded[i]);
                    }*/
                    callback(null, loaded, (totalSize / DOC_SIZE), PARTITION_INDEX-1, PARTITION_START-PARTITION_SIZE, PARTITION_SIZE, PARTITION_MAX); // no error, pass data
                } else {
                    console.error('Journal - Load Error:\n', err);
                    callback(true); // error
                }
            });
        }
    }

    J.reload = function(callback) {
        PARTITION_START = 0;
        PARTITION_INDEX = 0;
        PARTITION_SIZE = PARTITION_MAX;
        this.load(callback);
    }

    J.loadBlki = function(indices) {
        var blki = 0;
        var docs = [];
        var hits = 0; // cache hits
        var miss = 0; // cache misses
        var cached = {};
        var callback = function(err, loaded) { // gets one document
            if(!err) {
                docs.push(loaded[0]);
                CACHE.set(loaded[0]._blki, loaded[0]);
            } else {
                console.error('Journal - Load Error:\n', err);
            }
            //console.timeEnd('Journal - Block Load Time');
        }.bind(docs);

        console.log('Journal - Blocks to load', indices.length);
        for(var i = 0; i < indices.length; i++) {
            blki = indices[i];
            cached = CACHE.get(blki);
            if(cached) {
                hits++;
                docs.push(cached);
                continue; // skip loading from file
            }
            miss++;
            //console.log('Journal - Loading BLKI', blki);
            //console.time('Journal - Block Load Time');
            STORE.load(blki, 1, callback); // gets 1 document, synchronous call because mmap loads are blocking
        }

        console.error('Journal - Loaded Blocks - Cache: Hits', hits, 'Miss', miss, 'Total', docs.length);
        return docs;
    }

    return J;
}
