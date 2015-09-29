'use strict';
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
