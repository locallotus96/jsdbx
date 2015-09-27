var db = require('../LIB/JSDBX.js');

var cName = 'test';
db.connect('DB/TEST', [cName], function(err) {
    if(err) {
        console.log(err);
    } else {
        test();
    }
});

function test() {
  console.log('<DB> Current Collection: ', db[cName].C_NAME);
  console.log('<DB> Current Records:', db[cName].count());

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  for (var i = 0; i < 2500; i++) {
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

  //console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  /*db.disconnect('test', function(err) {
      if(err) {
          console.error(err);
      } else {

      }
  });*/
}
