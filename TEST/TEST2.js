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

  insertTestRecords(1000000, cName);

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
          name: 'Tim' + i,
          surname: 'Richards' + i,
          //password: crypto.randomBytes(8),
          score: i,
          teacher: 'Bob',
          city: 'Cape Town',
          age: Math.floor((Math.random() * 100) + 1),
          obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
      });
  };
  console.timeEnd(':: Insert Time');
  console.log('<DB> Records:', db[cName].count());
}
