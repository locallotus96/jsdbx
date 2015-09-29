var fs = require('fs');

var i = fs.openSync('DB/TEST/test.db', 'r');
var o = fs.openSync('DB/TEST/test.db.bak', 'w');

var buf = new Buffer(1024 * 1024), len, prev = '';

console.time('Copy Time');
while(len = fs.readSync(i, buf, 0, buf.length)) {

    var a = (prev + buf.toString('ascii', 0, len)).split('\n');
    prev = len === buf.length ? '\n' + a.splice(a.length - 1)[0] : '';

    var out = '';
    a.forEach(function(line) {

        if(!line)
            return;

        // do something with your line here

        out += line + '\n';
    });

    var bout = new Buffer(out, 'ascii');
    fs.writeSync(o, bout, 0, bout.length);
}
console.timeEnd('Copy Time');

fs.closeSync(o);
fs.closeSync(i);
