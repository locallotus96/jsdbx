'use strict';
var fs = require('fs');
var mmap = require('mmap-io');

/*
  NOTE
  Wiki: A memory-mapped file is a segment of virtual memory which has been assigned a direct byte-for-byte correlation with some portion of a file or file-like resource.

  The node mmap-io module allocates writable or readable buffers against portions of the file(s)
  When we insert data, a writable buffer gets allocated,
  from either a newly truncated section of bytes (block) in the file (appending),
  or an existing block when updating, or re-using space from deleted blocks.

  When we're done, in order to reflect the changes on disk, we need to perform a sync() call.
  Sync() call are expensive on indivdual blocks at the moment but good for batch inserts.
  TODO: Use fs.write() calls to write individual blocks when needed

  NOTE The collection in memory corresponds to a mmap read buffer of up to MAP_SIZE or
  the number of bytes in the partition we're loading.

  TODO When we update or insert at a deleted block,
  check if doc at blki offset is currently loaded/mapped in current buffer range,
  and write to this buffer at the appropriate offset.
  Otherwise only then allocate a new writable buffer, write to it and sync() it.
/*
/*
  NOTE: First time access of a mmap'ed section of the file is slow,
  consecutive accesses are much faster if the pages are still mapped and mostly
  reside in physical memory without having to be read from disk.
*/

module.exports = function(fd) {
  // PRIVATE VARIABLES
  var PAGESIZE = mmap.PAGESIZE;
  var PROT_READ = mmap.PROT_READ;
  var PROT_WRITE = mmap.PROT_WRITE;
  var MAP_SHARED = mmap.MAP_SHARED;
  var MADV = mmap.MADV_SEQUENTIAL;

  console.log('PAGESIZE | PROT_READ | PROT_WRITE | MAP_SHARED | MADV');
  console.log(PAGESIZE+'\t\t', PROT_READ+'\t', PROT_WRITE+'\t\t', MAP_SHARED+'\t', MADV);

  /*
    NOTE: Why 4KB / 4096 bytes for block size?
    If you look at the results of stat() / fstat(), the OS buffer blksize for I/O should be 4096
    MMAP's pagesize will be the same
    The pagesize is selected by the OS for maximum performance
  */
  var DOC_SIZE = 4096; // 32768/65535 => 32/64KB = max document size
  var BLOCK_SIZE = DOC_SIZE; // 1 doc per block for ease of use
  var DOCS_PER_BLOCK = BLOCK_SIZE / BLOCK_SIZE; // NOTE: Var not used => how many documents are contained in one block?
  var CONTAINER = strRepeat('0', BLOCK_SIZE); // A pre-built string for storing a single doc
  var FILES = getPartitionFiles(fd); // Loop through expected names and get all partition files available in an array // [fd || './test.mmap'];
  var MAP_SIZE = BLOCK_SIZE * 256000; // BLOCK_SIZE * 256000 = 1GB, don't map more than this per buffer

  var LOG_READ = false;
  var LOG_WRITE = false;
  var LOG_SYNC = true;

  // TODO: use these?
  var READ_BUFFER;
  var WRITE_BUFFER;

  var MM = {}; // exported object (public)

  MM.MAP_SIZE = MAP_SIZE;
  MM.DOC_SIZE = DOC_SIZE;

  // PUBLIC METHODS

  MM.create = function(fd) {
      fs.writeFileSync(fd, '', 'utf8'); // create empty file
  }

  MM.truncate = function(fd, size) {
      fs.truncateSync(fd, size); // truncate file to given size
  }

  MM.getFileSize = function(partition) {
      var fd = fs.openSync(FILES[partition] || FILES[FILES.length-1], 'r');
      var size = fs.fstatSync(fd).size;
      fs.closeSync(fd);
      return size;
  }

  // Get the size of all partition files together
  MM.getTotalSize = function() {
      var totalBytes = 0;
      var fd;
      for(var i = 0; i < FILES.length; i++) {
          fd = fs.openSync(FILES[i], 'r');
          totalBytes += fs.fstatSync(fd).size;
      }
      return totalBytes;
  }

  /**
  * Allocate and return a write buffer
  * @param {number} offset
  * @param {number} mSize
  * @param {number} partition
  */
  // NOTE: Map from byte offset till mSize of this partition aka file
  MM.mapWrite = function(offset, mSize) { // partition = partition no.
      // NOTE This works as long as offset is always a global byte offset not specific to any one file
      // With reading it's trickier because we write across paritions
      // Get the partition no. by determining partition index based on: offset / mapSize => floored
      LOG_WRITE ? console.log('MMAP - Global offset', offset): false;
      var partitionIndex = Math.floor(offset / MAP_SIZE);
      var file = FILES[partitionIndex];
      // Now we need to determine if the offset should start at 0 for this partition (offset can span multiple partitions)
      if(offset % MAP_SIZE === 0)
          offset = 0;
      else
          offset = offset % MAP_SIZE; // 512 % 1024 = 512 (part 0), 1536 % 1024 = 512 (part 1)

      LOG_WRITE ? console.log('MMAP - Mapping from', file, 'at offset', offset, 'to', mSize): false;

      LOG_WRITE ? console.time('MMAP - Allocate Write Buffer Time'): false;
      var fdW = fs.openSync(file, 'r+'); // write partition depending on: offset / MAP_SIZE => floored
      var wBuffer = mmap.map(mSize || MAP_SIZE, PROT_WRITE, MAP_SHARED, fdW, offset, MADV);
      mmap.advise(wBuffer, 0, PAGESIZE, MADV); // NOTE Does this even affect anything?
      fs.closeSync(fdW);
      LOG_WRITE ? console.timeEnd('MMAP - Allocate Write Buffer Time'): false;
      return wBuffer;
  }

  /**
  * Allocate and return a read buffer
  * @param {number} offset
  * @param {number} mSize
  */
  // NOTE: Map from byte offset till mSize, offset is global byte offset across files/partitions (See block indices: _blki * BLOCK_SIZE)
  MM.mapRead = function(offset, mSize) { // partition = partition no.
      // NOTE This works as long as offset is always a global byte offset not specific to any one file
      // With reading it's trickier because we write across paritions
      // Get the partition no. by determining partition index based on: offset / mapSize => floored
      LOG_READ ? console.log('MMAP - Global offset', offset) : false;
      var partitionIndex = Math.floor(offset / MAP_SIZE);
      var file = FILES[partitionIndex];
      // Now we need to determine if the offset should start at 0 for this partition (offset can span multiple partitions)
      if(offset % MAP_SIZE === 0)
          offset = 0;
      else
          offset = offset % MAP_SIZE; // 512 % 1024 = 512 (part 0), 1536 % 1024 = 512 (part 1)

      LOG_READ ? console.log('MMAP - Mapping from', file, 'at offset', offset): false;

      LOG_READ ? console.time('MMAP - Allocate Read Buffer Time') : false;
      var fdR = fs.openSync(file, 'r');
      var rBuffer = mmap.map(mSize || MAP_SIZE, PROT_READ, MAP_SHARED, fdR, offset, MADV);
      mmap.advise(rBuffer, 0, PAGESIZE, MADV); // NOTE Does this even affect anything?
      fs.closeSync(fdR);
      LOG_READ ? console.timeEnd('MMAP - Allocate Read Buffer Time') : false;
      return rBuffer;
  }

  /**
  * Sync a write buffer back to disk
  * @param {buffer} wBuffer
  * @param {number} offset
  */
  // NOTE: Never set byte offset to anything other than 0, unless you want to sync only a subset of the buffer from offset till mSize
  // If you don't specify an offset, then do not specify mSize either
  MM.mapSync = function(wBuffer, offset, mSize) {
      var blocking = true;
      LOG_SYNC ? console.log('MMAP - Blocking Sync:', blocking): false;
      LOG_SYNC ? console.time('MMAP - Sync Time'): false;
      // NOTE Without calling advise() first, syncs are 3 to 7x slower
      mmap.advise(wBuffer, 0, PAGESIZE, MADV);
      try {
          // NOTE: PARAMS: mmap.sync(writeBuffer, offset, length, blocking, invalidate)
          mmap.sync(wBuffer, offset || 0, mSize || MAP_SIZE, blocking, false);
      } catch(e) {
          //console.log(e); // this always seems to give an error but we are good
      }
      LOG_SYNC ? console.timeEnd('MMAP - Sync Time'): false;
  }

  /**
  * Load data from file starting at 'offset', for 'count' no. of blocks
  * @param {number} offset
  * @param {number} count
  * @param {function} callback
  */
  // NOTE: offset is the block/doc number to start loading from for count no. of blocks
  // These values are coerced to byte offsets
  MM.load = function(offset, count, callback) {
      var rBuffer = this.mapRead(offset*BLOCK_SIZE, count*BLOCK_SIZE);
      // block = reference to subset of buffer memory space for a single doc
      var block;
      var docList = [];
      LOG_READ ? console.time('MMAP - Read and Parse Blocks from Buffer Time') : false;
      for(var i = 0; i < count*BLOCK_SIZE; i += BLOCK_SIZE) {
          block = rBuffer.slice(i, i+BLOCK_SIZE); // get from buffer
          docList.push(getDocFromBlock(block.toString()));
      }
      LOG_READ ? console.timeEnd('MMAP - Read and Parse Blocks from Buffer Time') : false;
      //console.log('Docs Loaded', docList.length);
      callback(null, docList);
      //return docList; // return because MMAP read is synchronous so callback is not needed
  }

  /**
  * Write incoming data to file in various ways depending on some criteria
  * @param {array} docList
  * @param {array} freeBlocks
  * @param {function} callback
  * @param {boolean} update
  */
  // Appending to, or overwriting free (updated / deleted) block space...
  MM.write = function(docList, callback, update, fill) {
      console.log('MMAP - Docs to insert', docList.length, 'Updating?', update, 'Re-using Blocks?', fill || false);
      if(update) { // updating in-order block indices
          var offset;
          // Get the size of the mapping needed to fit the new docs
          var mapSize = BLOCK_SIZE; // individual doc during update
          console.log('MMAP - Update Map Size:', mapSize);
          var wBuffer;
          for(var i = 0; i < docList.length; i++) {
              console.log('MMAP - Overwriting blki', docList[i]._blki);
              offset = docList[i]._blki * BLOCK_SIZE; // offset is a multiple of block index and doc size
              wBuffer = this.mapWrite(offset, mapSize);
              new Buffer(getWritableDoc(docList[i])).copy(wBuffer, 0, 0, wBuffer.length);
              // Now overwrite doc in file
              this.mapSync(wBuffer, 0);
          }
          callback(false); // no error
      } else if(fill && docList.length <= fill) { // NOTE filling deleted block space TODO Should take all deleted space then start appending (when not batch inserting)
          var offset;
          // Get the size of the mapping needed to fit the new docs
          var mapSize = BLOCK_SIZE; // individual doc when filling deleted block space
          console.log('MMAP - Insert Map Size:', mapSize);
          var wBuffer;
          var doc = {};
          for(var i = 0; i < fill; i++) {
              console.log('MMAP - Overwriting blki', docList[i]._blki);
              offset = docList[i]._blki * BLOCK_SIZE; // offset is a multiple of block index and doc size
              wBuffer = this.mapWrite(offset, mapSize);
              new Buffer(getWritableDoc(docList.shift())).copy(wBuffer, 0, 0, wBuffer.length);
              // Now overwrite doc in file
              this.mapSync(wBuffer, 0); // TODO sync afterwards as much as possible within currenly loaded partition
          }
          if(docList.length === 0) { // TODO: we're done, no need to append anything
              callback(false); // no error
              return;
          } else {
              callback(false); // no error
              return;
          }
      } else { // appending to end of file (aka last partition)
          // Get the total file size to determine the byte offset to map from
          // NOTE Blocks must be sorted by _blki
          var offset = docList[0]._blki * BLOCK_SIZE; //this.getTotalSize();
          // Get the size of the mapping needed to fit the new docs
          var mapSize = docList.length * BLOCK_SIZE; // no of docs to append by doc size
          console.log('MMAP - Append Map Size:', mapSize + ' Offset', offset);
          // Truncate the file to length needed for the new docs

          // check if the last partition can hold this range of new data
          var nextPartSize = 0;
          var freeBytes = 0; // free space left in last partition
          freeBytes = MAP_SIZE - (offset % MAP_SIZE); // ok file has this free space
          //if(!FILES[Math.floor((offset + mapSize - 1) / MAP_SIZE)]) { // because (512 + 512 - 1) / 1024 == 0, so we don't need the next partition yet
          if(freeBytes === MAP_SIZE) { // means that free space is zero
              nextPartSize = freeBytes;
              freeBytes = 0;
          } else {
              //freeBytes = mapSize - subPartSize;
              nextPartSize = mapSize - freeBytes;
              if(nextPartSize < 0) // there's more free space than needed or last partition is full
                  nextPartSize = 0;
          }
          if(mapSize >= freeBytes + nextPartSize) { // do we need to write more than we can fit in last partition?
              freeBytes = MAP_SIZE - (offset % MAP_SIZE);
              console.log('MMAP - Allocating New File(s) - Expanding Partition');
              var file = FILES[0] + FILES.length;
              console.log('MMAP - Creating', file);
              FILES.push(file); // new file with incremented name
              this.create(file); // initialize file to 0 bytes
              // We need to fill up the existing file and write remaining data to next file
              // truncate new file to length needed for sub-part
              //subPartSize = mapSize - (MAP_SIZE - offset); // we need (max file bytes - used bytes) = free space in file
              //subPartSize = (MAP_SIZE - offset) - mapSize;
              //subPartSize = mapSize - freeBytes;
              this.truncate(FILES[FILES.length-1], nextPartSize);
              console.log('MMAP - New File Truncated Size:', nextPartSize);
          }
          if(!FILES[Math.floor(offset / MAP_SIZE)]) // we have to create a new file
          {
              var file = FILES[0] + FILES.length;
              console.log('MMAP - Creating', file);
              FILES.push(file); // new file with incremented name
              this.create(file); // initialize file to 0 bytes
              this.truncate(FILES[FILES.length-1], mapSize);
              console.log('MMAP - New File Truncated Size:', mapSize);
          }

          console.log(nextPartSize, freeBytes);

          if(mapSize < freeBytes + nextPartSize) { // we only need to write into 1 partition (file) which means 1 buffer and 1 sync
              console.log('MMAP - Writing to existing partition file');
              // truncate file to space needed / left over
              //this.truncate(FILES[FILES.length-1], freeBytes + (offset % MAP_SIZE));
              //console.log('MMAP - Truncated File Size:', freeBytes + (offset % MAP_SIZE));
              this.truncate(FILES[FILES.length-1], mapSize);
              console.log('MMAP - Truncated File Size:', mapSize);
              // Map from offset up to the no. bytes we need (end of file since it was truncated)
              // Now we have a writable buffer that can hold the docs and be synced back to file
              var wBuffer = this.mapWrite(offset, mapSize, FILES.length-1);
              // block = reference to subset of buffer memory space for a single doc
              var block;
              // mapSize should = wBuffer.length
              console.log('MMAP - Map Size === Buffer Size', mapSize === wBuffer.length);
              console.time('MMAP - Compose and Write Blocks into Buffer Time');
              for(var i = 0; i < mapSize; i += BLOCK_SIZE) {
                  block = wBuffer.slice(i, i+BLOCK_SIZE);
                  // copy the new buffer into the existing one
                  new Buffer(getWritableDoc(docList.shift())).copy(block, 0, 0, block.length);
                  // alternative pattern
                  /*new Buffer(getWritableDoc(docList.pop())).
                    copy(
                      wBuffer.slice(i, i+BLOCK_SIZE),
                      BLOCK_SIZE
                    );*/
              }
              console.timeEnd('MMAP - Compose and Write Blocks into Buffer Time');
              // Now write them to file
              this.mapSync(wBuffer, 0);
              callback(false); // no error
          } else if(mapSize < freeBytes + nextPartSize) {
              // NOTE Rare condition
              // We just have to write to newly created partition file since the old one was full or empty
              console.log('MMAP - Writing to new partition file');
              // Map from offset up to the no. bytes we need (end of file since it was truncated)
              // Now we have a writable buffer that can hold the docs and be synced back to file
              var wBuffer = this.mapWrite(offset, mapSize, FILES.length-1);
              // block = reference to subset of buffer memory space for a single doc
              var block;
              // mapSize should = wBuffer.length
              console.log('MMAP - Map Size === Buffer Size', mapSize === wBuffer.length);
              console.time('MMAP - Compose and Write Blocks into Buffer Time');
              for(var i = 0; i < mapSize; i += BLOCK_SIZE) {
                  block = wBuffer.slice(i, i+BLOCK_SIZE);
                  // copy the new buffer into the existing one
                  new Buffer(getWritableDoc(docList.shift())).copy(block, 0, 0, block.length);
              }
              console.timeEnd('MMAP - Compose and Write Blocks into Buffer Time');
              // Now write them to file
              this.mapSync(wBuffer, 0);
              callback(false); // no error
          } else { // here we have to write across 2 partitions (files) which means 2 buffers and 2 syncs
              console.log('MMAP - Writing across multiple partition files');
              // truncate old file to space needed / left over
              this.truncate(FILES[FILES.length-2], MAP_SIZE); // -2 because we had to create a new partition file
              console.log('MMAP - Filled Last Partition:', MAP_SIZE);
              // block = reference to subset of buffer memory space for a single doc
              var block;
              // Map from offset up to the no. bytes we need (end of file since it was truncated)
              // Now we have a writable buffer that can hold the docs and be synced back to file
              var wBuffer = this.mapWrite(offset, freeBytes, FILES.length-2); // write to end of this file
              console.time('MMAP - Compose and Write Blocks into Buffer Time');
              for(var i = 0; i < freeBytes; i += BLOCK_SIZE) {
                  block = wBuffer.slice(i, i+BLOCK_SIZE);
                  // copy the new buffer into the existing one
                  new Buffer(getWritableDoc(docList.shift())).copy(block, 0, 0, block.length);
              }
              console.timeEnd('MMAP - Compose and Write Blocks into Buffer Time');

              // Now write them to file
              this.mapSync(wBuffer, 0);

              var wBuffer = this.mapWrite(offset+freeBytes, nextPartSize, FILES.length-1); // here we write on the new file
              console.time('MMAP - Compose and Write Blocks into Buffer Time');
              for(var i = 0; i < nextPartSize; i += BLOCK_SIZE) {
                  block = wBuffer.slice(i, i+BLOCK_SIZE);
                  // copy the new buffer into the existing one
                  new Buffer(getWritableDoc(docList.shift())).copy(block, 0, 0, block.length);
              }
              console.timeEnd('MMAP - Compose and Write Blocks into Buffer Time');

              // Now write them to file
              this.mapSync(wBuffer, 0);
              callback(false); // no error
          }
      }
  }

  // NOTE: Function not used!
  MM.save = function(offset, docList, callback) {
      console.time('MMAP - Compose and Write Blocks into Buffer Time');
      // Get the size of the mapping needed to fit the new docs
      var mapSize = docList.length * BLOCK_SIZE;
      console.log('MMAP - Save Map Size:', mapSize);
      // Map from offset up to the no. bytes we need (end of file since it was truncated)
      // Now we have a writable buffer that can hold the docs and be synced back to file
      var wBuffer = this.mapWrite(offset, mapSize); // NOTE Beware of missing partition index here
      // block = reference to subset of buffer memory space for a single doc
      var block;
      // index no. blocks / no. docs
      var blki = 0;
      // mapSize should = wBuffer.length
      console.log('MMAP - Map Size === Buffer Size', mapSize === wBuffer.length);
      var doc = {};
      var docStr = '';
      for(var i = 0; i < mapSize; i += BLOCK_SIZE) {
          block = wBuffer.slice(i, i+BLOCK_SIZE);
          // copy the new buffer into the existing one
          doc = docList.pop();
          docStr = getWritableDoc(doc);
          new Buffer(docStr).copy(block, 0, 0, block.length);
          //console.log(blki, block.toString().length);
          /*new Buffer(getWritableDoc(docList.pop())).
            copy(
              wBuffer.slice(i, i+BLOCK_SIZE),
              BLOCK_SIZE
            );*/
          blki++;
      }
      console.timeEnd('MMAP - Compose and Write Blocks into Buffer Time');
      // Now write them to file
      this.mapSync(wBuffer, 0);
      callback(false); // no error
  }

  // PRIVATE HELPER FUNCTIONS

  // return a string padded to expected doc size,
  // the doc is stringyfied then padded to block length, and returned
  // CONTAINER is a prebuilt zero-padded string of block size,
  // it saves processing not having to make the string each time, because we copy the doc into it.
  function getWritableDoc(doc) {
      //return pad(makeStr(BLOCK_SIZE), doc, false); // false = don't pad on left
      //return pad(makeStr2(BLOCK_SIZE), doc, false); // false = don't pad on left
      return padStr(CONTAINER, JSON.stringify(doc) || '', false); // false = don't pad on left
  }

  // assuming multiple docs per block
  // return an array of extracted docs
  function getDocsFromBlock(blockStr) { // block = buffer object as string
      var docs = [];
      var doc;
      for(var i = 0; i < blockStr.length; i += BLOCK_SIZE) {
          doc = blockStr.slice(i, i+BLOCK_SIZE);
          //console.log(i, doc.length);
          //console.log(doc.substring(0, 200));
          // now remove trailing zero's from the padded doc string,
          // json parse it and add it to the array of doc objects
          docs.push(JSON.parse(unpadStr(doc, false)));
      }
      return docs;
  }

  // assuming 1 block = 1 doc
  function getDocFromBlock(blockStr) {
      return JSON.parse(unpadStr(blockStr, false));
  }

  // NOTE: A lot of processing time is spent here
  // fill and pad out a given string with 0's
  function padStr(pad, str, padLeft) {
      //if(typeof str !== 'string')
          //return pad;
      if(padLeft) {
          return (pad + str).slice(-pad.length);
      } else { // right padding
          return (str + pad).substring(0, pad.length);
      }
  }

  // remove zero padding from a given string
  // we actually return a substring of the parsed doc string
  function unpadStr(str, padLeft) {
      //if(typeof str !== 'string')
          //return '';
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
  function makeStr(len, char) {
      return Array(BLOCK_SIZE+1).join(char || '0');
  }

  // Array.prototype.join doesn't have to be called on an Array, just an Object with a length property
  // This lets you benefit from native function making the string, without the overhead of a huge array.
  // Very slow!
  function makeStr2(len, char) {
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

  /* END of pre-built string methods */

  // We've passed in fd to constructor, but this serves as base partition file, we may have more so we need to get them
  // by looping through their expected names until we hit a file we can't access.
  function getPartitionFiles(fd) {
      var count = 1;
      var validFiles = [fd]; // Partition index 0 is always base fd passed in
      while(true) { // find the other partition files
          try {
              fs.accessSync(fd + count, fs.F_OK);
              validFiles.push(fd + count);
              count++;
          } catch (err) {
              console.log('MMAP - Partition Files:', validFiles);
              return validFiles; // ok files to this point are valid, we've reached the end
          }
      }
  }

  return MM;
}//(); // self executing
