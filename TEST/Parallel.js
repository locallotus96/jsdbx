'use strict';

var Parallel = require('paralleljs');

var obj = {};
obj.a = 10;

obj.slowSquare = function (n) {
    var i = 0;
    while (++i < n * n) { }
    console.log('fine', this.a);
    return i;
};

var p;
//p = new Parallel([40000, 50000, 60000]); // NOTE 1 thread (always) using spawn
var p2;
p2 = new Parallel([40000, 50000, 60000], {maxWorkers: 3}); // NOTE 3 threads (configurable) using map, 1 thread per array element!

// -----------------------------------------------------------------------------
var start;
var time = null;

start = Date.now();

if(p) {
    p.spawn(function (data) { // NOTE 1 thread
        for (var i = 0; i < data.length; ++i) {
            var n = data[i];
            var square;
            for (square = 0; square < n * n; ++square) { }
            data[i] = square;
        }
        return data;
    }).then(function (data) {
        time = Date.now() - start;
        console.log(time);
    });
}

// -----------------------------------------------------------------------------
var start2;
var time2 = null;

start2 = Date.now();

if(p2) {
    p2.map(obj.slowSquare).then(function (data) { // NOTE 3 threads
        time2 = Date.now() - start2;
        console.log(time2);
    });
}

// -----------------------------------------------------------------------------
/*var start3;
var time3 = null;

start3 = Date.now();

var data = [10000, 20000, 30000, 40000, 50000, 60000];

for(var i = 0; i < data.length; i++) {
    for (var i = 0; i < data.length; ++i) {
        var n = data[i];
        var square;
        for (square = 0; square < n * n; ++square) { }
        data[i] = square;
    }
}
time3 = Date.now() - start3;
console.log(time3);

console.log(data);*/
