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

//--------------------------------------------------------------
function test() {
  console.log('<DB> Current Collection: ', db[cName].C_NAME);
  console.log('<DB> Current Records:', db[cName].count());

  insertTestRecords(2500000, cName);

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  db.disconnect(cName, function(err) {
      if(err) {
          console.error('<DB> Error:', err);
      } else {

      }
  });
}

function insertTestRecords(x, cName) {
  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  for (var i = 0; i < x; i++) {
      db[cName].insert({
          name: 'Buffer' + i,
          surname: 'Underrun' + i,
          //password: crypto.randomBytes(8),
          score: i,
          teacher: 'Tim',
          city: 'Cape Town',
          age: Math.floor((Math.random() * 10000) + 1)
      });
  };
  console.timeEnd(':: Insert Time');
  console.log('<DB> Records:', db[cName].count());
}
