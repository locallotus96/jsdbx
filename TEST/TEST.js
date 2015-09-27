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

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
var cName2 = 'test';
db.connect('DB/TEST', cName2, function(err) {
    if(err) {
        console.log('<DB> Error:', err);
    } else {
        test2();
    }
});

//--------------------------------------------------------------
function test() {
  console.log('<DB> Current Collection: ', db[cName].C_NAME);
  console.log('<DB> Current Records:', db[cName].count());

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  for (var i = 0; i < 1750000; i++) {
      db[cName].insert({
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
  console.log('<DB> Records:', db[cName].count());

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  db.disconnect(cName, function(err) {
      if(err) {
          console.error('<DB> Error:', err);
      } else {

      }
  });
}

//--------------------------------------------------------------
function test2() {
  console.log('<DB> Current Collection: ', db[cName2].C_NAME);
  console.log('<DB> Current Records:', db[cName2].count());

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  for (var i = 0; i < 10; i++) {
      db[cName2].insert({
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
  console.log('<DB> Records:', db[cName2].count());

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  db.disconnect(cName2, function(err) {
      if(err) {
          console.error(err);
      } else {

      }
  });
}
