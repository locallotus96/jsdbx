var DAL = require('./DAL.js');
var UTIL = require('./UTIL.js');

console.log('\n================LOADING COLLECTION======================');
console.time(':: Load Time');
/*for (var i = 0; i < 100000; i++) {
    DAL.insert({
        name: 'Foo' + i,
        surname: 'Bar' + i,
        //password: crypto.randomBytes(8),
        score: i,
        teacher: 'Tim'
    });
};*/
DAL.load('collection.txt', function() {
    if(DAL.COLLECTION.length > 0) {
        console.error('COLLECTION LOADED!');
        console.timeEnd(':: Load Time');
        console.log('No. Documents:', DAL.count());
        loaded();
    }
});

function loaded() {

console.log('\n======================================');
console.time(':: FindOne Time');
DAL.findOne({
    name: DAL.COLLECTION[Math.floor(DAL.count() / 2)].name
});
console.timeEnd(':: FindOne Time');

console.log('\n======================================');
console.time(':: Index on name Time');
DAL.createIndex('name');
console.timeEnd(':: Index on name Time');

console.log('\n======================================');
console.time(':: FindOne Time');
DAL.findOne({
    name: DAL.COLLECTION[DAL.count()-1].name
});
console.timeEnd(':: FindOne Time');

//var INDEXER = require('./INDEXER.js');

/*console.log('\n======================================');
console.log(INDEXER.INDECES['name'].contains(DAL.COLLECTION[DAL.count()-1].name));
console.log(INDEXER.INDECES['name'].get(DAL.COLLECTION[DAL.count()-1].name));
console.log(INDEXER.INDECES['name'].size());*/

console.log('\n======================================');
var oldName = DAL.COLLECTION[DAL.count()-1].name;
DAL.updateOne({name:oldName}, {name:'Bob'});
console.log('\n======================================');
DAL.updateOne({name:'Bob'}, {teacher:'Rudolf'});
console.log('\n======================================');
DAL.updateOne({name:'Foo10000'}, {name:'Bob'});

console.log('\n======================================');
console.time(':: Index on teacher Time');
DAL.createIndex('teacher');
console.timeEnd(':: Index on teacher Time');

//console.log('\n======================================');
//console.time(':: Index on score Time');
//DAL.createIndex('score');
//console.timeEnd(':: Index on score Time');

console.log('\n======================================');
console.time(':: Update Time');
DAL.updateOne({name:DAL.COLLECTION[DAL.count()-2].name}, {teacher:'Rudolf'});
console.timeEnd(':: Update Time');

console.log('\n======================================');
console.time(':: Update Time');
DAL.updateOne({name:DAL.COLLECTION[Math.floor(DAL.count()/2)].name}, {teacher:'Rudolf'});
console.timeEnd(':: Update Time');

console.log('\n======================================');
console.time(':: Remove Time');
DAL.removeOne({score:'99998'});
console.timeEnd(':: Remove Time');

console.log('\n======================================');
console.time(':: Remove Time');
DAL.remove({teacher:'Rudolf'});
console.timeEnd(':: Remove Time');

console.log('\n======================================');
console.time(':: Find Time');
DAL.findAny({
    name: 'Bob',
    teacher: 'Rudolf',
    score: '99997'
});
console.timeEnd(':: Find Time');

/*console.log('\n======================================');
console.log(INDEXER.INDECES['name'].contains(oldName));
console.log(INDEXER.INDECES['name'].get(oldName));
console.log(INDEXER.INDECES['name'].size());*/

/*console.log('\n======================================');
console.log(INDEXER.INDECES['name'].contains('Bob'));
console.log(INDEXER.INDECES['name'].get('Bob'));
console.log(INDEXER.INDECES['name'].size());*/

}
