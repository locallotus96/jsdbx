'use strict';
// Global Modules
var fs = require('fs');
var stream = require('stream');
var inherits = require('util').inherits;

//=====================================================
// Create a sink (where the data goes)
var WRITABLE = stream.Writable;

function Sink(options) {
    WRITABLE.call(this, options);
}
inherits(Sink, WRITABLE);

Sink.prototype._write = function (chunk, encoding, callback) {
    //TODO: Write to specific file here
    console.log(chunk.toString());
    callback();
}

// Create a Source stream to which to send new lines
var READABLE = stream.Readable;
var STREAM_INSERT = new READABLE; // SOURCE_INSERT
var STREAM_UPDATE = new READABLE; // SOURCE_UPDATE
var STREAM_DELETE = new READABLE; // SOURCE_DELETE

var SINK_INSERT = new Sink;
var SINK_UPDATE = new Sink;
var SINK_DELETE = new Sink;
//=====================================================

//--- ===================================================================
//--- FILE OPERATIONS ===================================================
//--- ===================================================================

var FILER = {};

// messes with multiple connections, so never set to true
FILER.busyWriteStreaming = false; // are we currently streaming to the file?

FILER.saveFileStream = function (fd, collection, callback) {
    if(collection.length === 0) {
        callback();
        return;
    }
    // streaming overwrites the file each new stream
    if(!this.busyWriteStreaming) {
        //this.busyWriteStreaming = true; // messes with multiple connections
        console.log('<=> Streaming to:', fd + ' Old File Size:', this.getFilesizeInMBytes(fd));
        console.time('<=> Write File Stream Time');
        this.streamLinesToFile(fd, collection, function(err) {
            FILER.busyWriteStreaming = false;
            console.log('<=> Write File Stream Error:', err + ' New File Size:', FILER.getFilesizeInMBytes(fd));
            console.timeEnd('<=> Write File Stream Time');
            callback(err);
        });
    } else {
        callback(':: Busy write streaming!'); // signal error (we're just busy)
        return;
    }
}

FILER.loadFileStream = function (fd, callback) {
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
                console.log('<=> Read File Stream Error:', err + ' File Size:', FILER.getFilesizeInMBytes(fd));
                callback(err, null);
            }
        });
    }
}

FILER.streamToFile = function (fd, data, callback) {
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
FILER.streamLinesToFile = function (fd, data, callback) {
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
FILER.streamFromFile = function (fd, callback) {
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

FILER.streamFromFile2 = function (fd, callback) {
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
FILER.streamFromFile3 = function (fd, callback) {
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
FILER.streamLinesFromFile = function (fd, callback) {
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
/*FILER.streamLinesFromFile2 = function (fd, callback) {
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
FILER.bufferWriteFileSync = function (fd, data, callback) {
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
FILER.bufferReadFileSync = function (fd, callback) {
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

FILER.readFromFileSync = function (fd) {
    return fs.readFileSync(fd, 'utf-8');
}

FILER.readFromFileAsync = function (fd, callback) {
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
FILER.appendToFileAsync = function (fd, data, callback) {
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
FILER.appendLineAsync = function (buffer, error, fd, callback) {
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
            FILER.appendLineAsync(buffer, err, fd, callback);
        }
    });
}

FILER.appendToFileSync = function (fd, data) {
    for(var i = 0; i < data.length; i++) {
        fs.appendFileSync(fd, JSON.stringify(data[i]) + '\n', {encoding: 'utf8', flag: 'a'});
    }
}

FILER.removeFileSync = function (fd) {
    if(fs.unlinkSync(fd)) {
        return true;
    } else {
        return false;
    }
}

FILER.removeFileAsync = function (fd, callback) {
    fs.unlink(fd, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

// check if file is visible to calling process
FILER.isValidPathSync = function (path) {
    try {
        fs.accessSync(path, fs.F_OK);
    } catch (err) {
        return false;
    }
    return true;
}

// check if file is visible to calling process
FILER.isValidPathAsync = function (path, callback) {
    fs.access(path, fs.F_OK, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

// abstraction for exposed function
FILER.isValidPath = function (path) {
    return this.isValidPathSync(path);
}


// check read and write permissions
FILER.canReadWriteSync = function (path) {
      try {
          fs.accessSync(path, fs.R_OK | fs.W_OK);
      } catch (err) {
          return false;
      }
      return true;
}

// check read and write permissions
FILER.canReadWriteAsync = function (path, callback) {
    fs.access(path, fs.R_OK | fs.W_OK, function(err) {
        if(err) {
            callback(true);
        } else {
            callback(false);
        }
    });
}

FILER.resetFileSync = function (fd) {
    //fs.writeFileSync(fd, '', 'utf8', {encoding: 'utf8', flag: 'a');
    fs.writeFile(fd, '', 'utf8', function(err) {
        if(err) {
            console.error(err);
            throw err;
        }
    });
}

FILER.getFilesizeInBytes = function (fd) {
    try {
      var stats = fs.statSync(fd);
      return stats['size'];
    } catch (err) {
        return false;
    }
}

FILER.getFilesizeInMBytes = function (fd) {
    try {
        var stats = fs.statSync(fd);
        return (stats['size'] / 1024.0) / 1024.0;
    } catch (err) {
        return false;
    }
}

module.exports = FILER;
