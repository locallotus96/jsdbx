var DAL = require('./DAL.js');
var UTIL = require('./UTIL.js');

DAL.FILE = 'collection.db';

console.log('\n================ LOADING COLLECTION ======================');
console.time(':: Load Time');
DAL.load(function() {
    if(DAL.COLLECTION.length > 0) {
        console.timeEnd(':: Load Time');
        console.log('No. Documents:', DAL.count());
        test();
        console.log('=== Tests Complete - Saving... ===');
        DAL.save();
    } else {
        for (var i = 0; i < 100000; i++) {
            DAL.insert({
                name: 'Foo' + i,
                surname: 'Bar' + i,
                //password: crypto.randomBytes(8),
                score: i,
                teacher: 'Tim'
            });
        };
        test();
        console.log('=== Tests Complete - Saving... ===');
        DAL.save();
    }
});

function test() {
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

  console.log('\n======================================');
  var oldName = DAL.COLLECTION[DAL.count()-1].name;
  console.time(':: Update One Time');
  DAL.updateOne({name:oldName}, {name:'Bob'});
  console.timeEnd(':: Update One Time');
  console.log('\n======================================');
  console.time(':: Update One Time');
  DAL.updateOne({name:'Bob'}, {teacher:'Rudolf'});
  console.timeEnd(':: Update One Time');

  console.log('\n======================================');
  console.time(':: Index on teacher Time');
  DAL.createIndex('teacher');
  console.timeEnd(':: Index on teacher Time');

  console.time(':: Update One Time');
  DAL.updateOne({teacher:'Rudolf'}, {name:'Julian'});
  console.timeEnd(':: Update One Time');

  console.log('\n======================================');
  console.time(':: Remove One Time');
  DAL.removeOne({score:'99998'});
  console.timeEnd(':: Remove One Time');

  console.log('\n======================================');
  console.time(':: Remove Time');
  DAL.remove({teacher:'Rudolf'});
  console.timeEnd(':: Remove Time');

  console.log('\n======================================');
  console.time(':: Find Any Time');
  DAL.findAny({
      name: 'Bob',
      teacher: 'Rudolf',
      score: '99997'
  });
  console.timeEnd(':: Find Any Time');
}
