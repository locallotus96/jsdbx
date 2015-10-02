var db = require('../LIB/JSDBX.js');

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
var cName = 'test';
db.connect('DB/TEST', cName, function(err) {
    if(err) {
        console.log(err);
    } else {
        test();
    }
});

function test() {
    insertTestRecords(100000, cName);
    console.time('Find Time');
    var list = db.test.find();
    console.timeEnd('Find Time');
    console.log(list.length);

    console.time('Reverse (for push then splice) Time');
    reverseList(list); // very slow
    console.timeEnd('Reverse (for push then splice) Time');
    console.time('Reverse (native method) Time');
    list.reverse(); // amlost instant
    console.timeEnd('Reverse (native method) Time');

    console.time('Copy (while loop) Time');
    copyList(list); // very good
    console.timeEnd('Copy (while loop) Time');
    console.time('Copy (slice) Time');
    list.slice(); // almost instant
    console.timeEnd('Copy (slice) Time');
}

function insertTestRecords(x, cName) {
    console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
    console.time(':: Insert Time');
    for (var i = 0; i < x; i++) {
        db[cName].insert({
            name: 'Tim' + i,
            surname: 'Richards' + i,
            //password: crypto.randomBytes(8),
            score: i, // index
            teacher: 'Bob',
            city: 'Cape Town',
            age: Math.floor((Math.random() * 100) + 1),
            iq: Math.floor((Math.random() * 2000) - 1000),
            obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
        });
    };
    console.timeEnd(':: Insert Time');
    console.log('<DB> Records:', db[cName].count());
}

// in place
var reverseList = function (list) {
    var length = list.length;
    for(length -= 2; length > -1; length -= 1) {
          list.push(list[length]);
          list.splice(length, 1);
    }
    return list;
}

// copy a list
var copyList = function (list) {
    var i = list.length;
    var b = [];
    while (i--) b[i] = list[i];
    return b;
}
