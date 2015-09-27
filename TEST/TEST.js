var db = require('../LIB/JSDBX.js');

db.connect('DB/TEST', ['test']);

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
console.log('<DB> Collection: ', db.test.COLLECTION_NAME);
console.log('<DB> Records:', db.test.count());

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
console.time(':: Insert Time');
for (var i = 0; i < 100000; i++) {
    db.test.insert({
        name: 'Buffer' + i,
        surname: 'Underrun' + i,
        //password: crypto.randomBytes(8),
        score: i,
        teacher: 'Tim',
        city: 'Cape Town',
        age: Math.floor((Math.random() * 100) + 1)
    });
};
console.timeEnd(':: Insert Time');
console.log('<DB> Records:', db.test.count());

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
db.disconnect('test');
