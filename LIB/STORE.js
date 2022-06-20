'use strict';
var fs = require('fs');
var store = require('fd-chunk-store');

var CS = {};
/*
  NOTE: 256|512 KB blocks yields the best performance on my machine
*/
CS.DOC_SIZE = 32768; //32768/65535 32/64KB, max document size
CS.BUCKET_DEPTH = 16; // 524288 Bytes/512KB at a time in one block, fastest
CS.BLOCK_SIZE = CS.DOC_SIZE * CS.BUCKET_DEPTH; // buffer / string size needed for a block = no. bytes
CS.CONTAINER = strRepeat('0', CS.DOC_SIZE); // A pre-built string for storing a single doc

// max pending block writes we can have at a time through store.put(), before we run out of listeners for the events,
// changing this tweaks memory footprint because we buffer BLOCK_SIZE of data for each listener
CS.MAX_LISTENERS = 8; // 8 x 512KB = 4096KB/4MB
// count of listeners dispatched through store.put()
CS.LISTENERS = 0;
// bucket with pending stringified and zero-padded documents to write
CS.BUCKET = [];
// block index to start writing/appending at
CS.BLOCK_INDEX = 0;
// free block index to overwrite due to deletes
CS.BLOCK_INDEX_FREE = 0;
// collection of docs to write to file
CS.PENDING_DOCS = [];
// metrics
CS.TOTAL_WRITES_QUEUED = 0;
CS.TOTAL_WRITES_DONE = 0;

// NOTE: This is just in attempt to get more consistent writes
// Pausing on < 4GB data slows it down, very much so on < 2GB
CS.ENABLE_PAUSE = false;
CS.PAUSE_INTERVAL = 16; // target no. writes before pausing
CS.PAUSE_TIME = 2500; // ms
CS.WRITES_BEFORE_PAUSE = 0; // cumulative writes before pausing, resets on pause

CS.FILE = './test.store'; // default but should be passed as parameter in constructor

CS.STORE; // created in constructor

console.log('Doc Size', CS.DOC_SIZE);
console.log('Bucket Size', CS.BUCKET_DEPTH);
console.log('Block Size', CS.BLOCK_SIZE);

var firstWriteTime = false;

// TODO: Fix remaing 16 records not being written...

CS.write = function (records, freeIndices, callback) {
    if(CS.PENDING_DOCS.length > 0) {// this should mean we're currently writing
        CS.PENDING_DOCS = CS.PENDING_DOCS.concat(records);
        //console.log('Appending to write queue...', CS.PENDING_DOCS.length, CS.BUCKET.length);
    } else {
        CS.PENDING_DOCS = CS.PENDING_DOCS.concat(records);
        //console.log('Firing up write function...', CS.PENDING_DOCS.length, CS.BUCKET.length);
        if(!firstWriteTime)
            firstWriteTime = new Date();
        CS.writeBlocks(records, freeIndices, callback);
    }
}

CS.writeBlocks = function (records, freeIndices, callback) {
    if(CS.BUCKET.length === CS.BUCKET_DEPTH || CS.PENDING_DOCS.length < CS.BUCKET_DEPTH) {
        if(CS.LISTENERS < CS.MAX_LISTENERS) { // we can dispatch another listener through store.put()
            //console.log('Bucket length', CS.BUCKET.length);
            var block = ''; // block is an entire bucket's contents as a string we can make a buffer from
            //block = BUCKET.join(''); // leave out the commas with ''
            // faster than block = BUCKET.join('')
            for(var j = 0; j < CS.BUCKET.length; j++) {
                block = block.concat(CS.BUCKET[j]);
            }
            CS.BUCKET = []; // clear the BUCKET for next cycle
            // TODO
            /*if(freeIndices.length > 0) {
                CS.BLOCK_INDEX_FREE = Math.floor(freeIndices.pop() / 16); // get the block in which this record belongs
                // Now we need to retrieve the block, and insert the record at the appropriate point in the block, then write it back.
            }*/
            CS.TOTAL_WRITES_QUEUED++;
            CS.LISTENERS++; // we're dispatching
            //console.log(' Writing block index|length', CS.BLOCK_INDEX, block.length);
            CS.STORE.put(CS.BLOCK_INDEX, new Buffer(block), function(err) {
                CS.TOTAL_WRITES_DONE++;
                CS.WRITES_BEFORE_PAUSE++; // NOTE
                CS.LISTENERS--; // ok we done with this listener, decrement global counter
                //console.log('> Writes Done|Queued|Pending|Listening', CS.TOTAL_WRITES_DONE, CS.TOTAL_WRITES_QUEUED, CS.PENDING_DOCS.length, CS.LISTENERS);
                if(err) {
                    console.error('Error writing block:', err);
                }
                if(CS.PENDING_DOCS.length === 0 && CS.LISTENERS === 0) {
                    console.log('> Last write complete!');
                    console.log('Total Write time:', new Date() - firstWriteTime);
                    callback();
                }
            });
            CS.BLOCK_INDEX++; // increment block index
            //CS.BLOCK_INDEX = CS.BLOCK_INDEX + 1; // same as above, just less clear
            if(CS.PENDING_DOCS.length > CS.BUCKET_DEPTH) {
                CS.writeBlocks(CS.PENDING_DOCS, freeIndices, callback); // free up the node engine here with process.nextTick()
            } //else {
                //console.log(':: DONE DISPATCHING WRITERS - No more docs to write!');
            //}
        } else {
            var pauseStart = new Date();
            if(CS.ENABLE_PAUSE && CS.WRITES_BEFORE_PAUSE >= CS.PAUSE_INTERVAL) {
                CS.WRITES_BEFORE_PAUSE = 0;
                var _flagCheckA = setInterval(function() {
                    if(new Date() - pauseStart > CS.PAUSE_TIME) {
                        console.log(':: DONE PAUSING - Dispatching writers...');
                        clearInterval(_flagCheckA);
                        CS.writeBlocks(CS.PENDING_DOCS, freeIndices, callback);
                    }
                }, 250);
            } else {
                var _flagCheck = setInterval(function() {
                    //console.log(':: WAITING - Flag check for no. listeners', CS.LISTENERS);
                    // We let all writes finish before firing up again, underneath writing is technically synchronous
                    // Remember async is there so we don't wait for background operations in the event loop,
                    // so it can keep rolling and executing code
                    if (CS.LISTENERS === 0) { // CS.LISTENERS < CS.MAX_LISTENERS
                        //console.log(':: DONE WAITING - Dispatching writers...');
                        clearInterval(_flagCheck);
                        CS.writeBlocks(CS.PENDING_DOCS, freeIndices, callback);
                    }
                }, 0); // interval set at 50 milliseconds
            }
        }
    } else {
        // fill the bucket with documents to write
        //console.log('Filling bucket...', CS.BUCKET.length, CS.PENDING_DOCS.length);
        if(CS.PENDING_DOCS.length < CS.BUCKET_DEPTH) { // pad the block to write
            console.log(' Last block needs padding...');
            console.log(' Filling last bucket...', CS.BUCKET.length, CS.PENDING_DOCS.length);
            while(CS.BUCKET.length < (CS.BUCKET_DEPTH-CS.PENDING_DOCS.length)) {
                CS.BUCKET.push(CS.getWritableDoc(CS.PENDING_DOCS.shift()));
            }
            console.log(' Filled last bucket...', CS.BUCKET.length, CS.PENDING_DOCS.length);

            for(var k = CS.BUCKET.length; k < CS.BUCKET_DEPTH; k++) {
                block = block.concat(CS.getWritableDoc({})); // pad with empty doc
            }
            console.log(' Last block length', block.length);
        } else {
            for(var i = 0; i < CS.BUCKET_DEPTH; i++) {
                //CS.BUCKET.push(CS.getWritableDoc(CS.PENDING_DOCS.shift()));
                CS.BUCKET.push(CS.getWritableDoc(CS.PENDING_DOCS.pop())); // pop is much faster than shift, --array.length is fastest
            }
        }
        //console.log('Filled bucket...', CS.BUCKET.length, CS.PENDING_DOCS.length);
        CS.writeBlocks(CS.PENDING_DOCS, freeIndices, callback);
    }
}

/*
  Here we get a subset of the contents in the store,
  so we can have this subset as a collection to work on in memory,
  while lazily persisting changes to disk
*/
CS.load = function (startDocIndex, endDocIndex, callback) {
    /* We want to get documents 0 to 1024, there's 16 docs in a block,
       therefore we read 64 blocks. */
    var blockRange = Math.floor((endDocIndex - startDocIndex) / 16);
    var startBlockIndex = Math.floor(startDocIndex / 16);
    var endBlockIndex = startBlockIndex + blockRange;
    console.log('Reading block range', startBlockIndex, endBlockIndex);
    CS.getBlocks(startBlockIndex, endBlockIndex, [], callback);
}

// Helper function for CS.load()
// Recusively read all blocks containing docs in the requested range
CS.getBlocks = function (currentBlockIndex, endBlockIndex, collection, callback) {
    CS.STORE.get(currentBlockIndex, function(err, buf) {
        // buffer will have 64 documents
        // now we extract them and push them into the collection
        if(err) {
            console.log('Error getting block:', err);
        } else {
            //console.log(buf.toString().slice(0, buf.length/16));
            //console.log(currentBlockIndex, buf.toString().slice(0, buf.length/16).length);
            if(buf.length > 0) {
                collection = collection.concat(CS.getDocsFromBlock(buf.toString()));
                if(currentBlockIndex < endBlockIndex) {
                    currentBlockIndex++;
                    CS.getBlocks(currentBlockIndex, endBlockIndex, collection, callback);
                } else {
                    console.log('Done reading ' + currentBlockIndex+1, 'blocks!');
                    CS.BLOCK_INDEX = currentBlockIndex;
                    callback(null, collection);
                }
            } else {
                console.log('Done reading ' + currentBlockIndex, 'blocks! Empty buffer reached!');
                CS.BLOCK_INDEX = currentBlockIndex;
                callback(null, collection);
            }
        }
    });
}

// return a string padded to expected doc size
CS.getWritableDoc = function (doc) {
    //return pad(makeStr(DOC_SIZE), doc, false); // false = don't pad on left
    //return pad(makeStr2(DOC_SIZE), doc, false); // false = don't pad on left
    return CS.pad(CS.CONTAINER, JSON.stringify(doc) || '', false); // false = don't pad on left
}

CS.getDocsFromBlock = function (block) { // block = buffer object as string
    var docs = [];
    var doc;
    for(var i = 0; i < block.length; i+=CS.DOC_SIZE) {
        doc = block.slice(i, i+CS.DOC_SIZE);
        //console.log(i, doc.length);
        //console.log(doc.substring(0, 200));
        // now remove trailing zero's from the padded doc string,
        // json parse it and add it to the array of doc objects
        docs.push(JSON.parse(CS.unpad(doc, false)));
    }
    return docs;
}

// NOTE: A lot of processing time is spent here
// fill and pad out a given string
CS.pad = function (pad, str, padLeft) {
    if(typeof str !== 'string')
        return pad;
    if(padLeft) {
        return (pad + str).slice(-pad.length);
    } else { // right padding
        return (str + pad).substring(0, pad.length);
    }
}

// remove zero padding from a given string
CS.unpad = function (str, padLeft) {
    if(typeof str !== 'string')
        return '';
    if(padLeft) {
        return str.substring(str.indexOf('{'), str.length);
    } else { // right padding
        return str.substring(0, str.lastIndexOf('}')+1);
    }
}

/*
  Methods for returning a pre-built string
  to fill with data using pad()
*/
// fast-ish
CS.makeStr = function (len, char) {
    return Array(CS.DOC_SIZE+1).join(char || '0');
}

// Array.prototype.join doesn't have to be called on an Array, just an Object with a length property
// This lets you benefit from native function making the string, without the overhead of a huge array.
// Very slow!
CS.makeStr2 = function (len, char) {
    return Array.prototype.join.call({length: (len || -1) + 1}, char || '0');
}

// fastest way, method from underscore.string
function strRepeat(str, qty) {
    if (qty < 1) return '';
    var result = '';
    while (qty > 0) {
        if (qty & 1) result += str;
        qty >>= 1, str += str;
    }
    return result;
}

module.exports = function (fd) {
    CS.FILE = fd;
    CS.STORE = new store(CS.BLOCK_SIZE, CS.FILE);
    return CS;
}
