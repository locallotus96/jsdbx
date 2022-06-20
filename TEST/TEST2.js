var db = require('../LIB/JSDBX.js');

/*var gc = new (require('gc-stats'))();

gc.on('stats', function(stats) {
    console.log('GC Happened', stats);
});*/

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
var cName = 'test';
db.connect('DB/TEST', cName, function(err) {
    if(err) {
        console.log(err);
    } else {
        test(cName);
    }
});
/*var c2Name = 'test2';
db.connect('DB/TEST', c2Name, function(err) {
    if(err) {
        console.log(err);
    } else {
        test(c2Name);
    }
});
var c3Name = 'test3';
db.connect('DB/TEST2', c3Name, function(err) {
    if(err) {
        console.log(err);
    } else {
        test(c3Name);
    }
});*/

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
db.disconnect(cName, function(err) {
    if(err) {
        console.error(err);
    } else {

    }
});

//--------------------------------------------------------------
function test(cName) {
  console.log('<DB> Current Collection: ', db[cName].CNAME);
  console.log('<DB> Current Records:', db[cName].count());
  console.log('<DB> Current Records (loaded):', db[cName].loaded());

  //insertTestRecords(0, 512000, cName);
  //insertTestRecords(256000, 512000, cName);
  //insertTestRecords(512000, 256000, cName);

  //insertTestRecords(0, 192000, cName);
  //insertTestRecords(192000, 128000, cName);
  //insertTestRecords(320000, 128000, cName);
  //insertTestRecords(448000, 128000, cName);

  /*console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  db[cName].insert({
      name: 'Corne',
      surname: 'Rossouw',
      //password: crypto.randomBytes(8),
      score: 91, // index
      teacher: 'Trevor',
      city: 'Cape Town',
      age: 24,
      iq: 1024*4
      //obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
  });
  console.timeEnd(':: Insert Time');
  console.log('<DB> Records:', db[cName].count());*/

  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Create Indices Time');
  db[cName].createIndex(['score', 'name', 'iq', 'age']);
  console.timeEnd(':: Create Indices Time');
  console.log('Index Size score:', db[cName].indexSize('score'));
  console.log('Index Size name:', db[cName].indexSize('name'));
  console.log('Index Size iq:', db[cName].indexSize('iq'));
  console.log('Index Size age:', db[cName].indexSize('age'));
  //console.log('Index Size city:', db[cName].indexSize('city'));
  //console.log('Index Size teacher:', db[cName].indexSize('teacher'));
  console.log('Total Index Size:', db[cName].indexSize(['score', 'name', 'iq', 'age']));

  var startTime = new Date();
  console.time(':: Total Query Time');

  console.log('1|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 32000-1}));
  console.timeEnd(':: Find Time');

  console.log('2|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findAny({name: 'Tim63999', score: 64000-1}));
  console.timeEnd(':: Find Time');

  console.log('3|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 96000-1}));
  console.timeEnd(':: Find Time');

  console.log('4|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 128000-1}));
  console.timeEnd(':: Find Time');

  console.log('5|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 192000-1}));
  console.timeEnd(':: Find Time');

  console.log('6|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 256000-1}));
  console.timeEnd(':: Find Time');

  console.log('7|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({iq: 750}).length);
  console.timeEnd(':: Find Time');

  console.log('8|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({age: 75}).length);
  console.timeEnd(':: Find Time');

  console.log('9|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 320000-1}));
  console.timeEnd(':: Find Time');

  console.log('10|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 384000-1}));
  console.timeEnd(':: Find Time');

  console.log('11|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findOne({score: 448000-1}));
  console.timeEnd(':: Find Time');

  console.log('12|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({iq: 500}).length);
  console.timeEnd(':: Find Time');

  console.log('13|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findAny({score: 194000, name:'Tim450000'}).length);
  console.timeEnd(':: Find Time');

  console.log('14|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findAny({iq: 400, score: 194000, name:'Tim450000'}).length);
  console.timeEnd(':: Find Time');

  console.log('15|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({name: 'Tim128000', score:128000}).length);
  console.timeEnd(':: Find Time');

  console.log('16|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({name: 'Tim128000'}).length);
  console.timeEnd(':: Find Time');

  console.log('17|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findAny({iq:225}).length);
  console.timeEnd(':: Find Time');

  console.log('18|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({score: 0}).length);
  console.timeEnd(':: Find Time');

  console.log('19|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({score: 91}).length);
  console.timeEnd(':: Find Time');

  console.log('20|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].findAny({name: 'Corne', score: -95}));
  console.timeEnd(':: Find Time');

  console.log('21|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({iq: 99}).length);
  console.timeEnd(':: Find Time');

  console.log('22|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({iq: 500}).length);
  console.timeEnd(':: Find Time');

  console.log('23|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({age: 100}).length);
  console.timeEnd(':: Find Time');

  console.log('24|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({age: 100}).length);
  console.timeEnd(':: Find Time');

  console.log('25|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Find Time');
  console.log(db[cName].find({iq: 554}).length);
  console.timeEnd(':: Find Time');

  //console.log('26|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  //console.time(':: Find Time');
  //console.log(db[cName].findOne({name: 'Tim384000', surname: 'Richards384000'}));
  //console.timeEnd(':: Find Time');


  /*
  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  db[cName].insert({
      name: 'Corne',
      surname: 'Rossouw',
      //password: crypto.randomBytes(8),
      score: -95, // index
      teacher: 'Trevor',
      city: 'Cape Town',
      age: 24,
      iq: 1024*4
      //obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
  });
  console.timeEnd(':: Insert Time');
  console.log('<DB> Records:', db[cName].count());*/

  console.log('Average Query Time', (new Date() - startTime) / 25, 'ms'); // time taken / total queries
  console.log('Queries Per Second', 1000 / ((new Date() - startTime) / 25)); // time taken / total queries
  console.timeEnd(':: Total Query Time');

  console.log('<DB> Current Records:', db[cName].count());
  console.log('<DB> Current Records (loaded):', db[cName].loaded());
}

function insertTestRecords(j, x, cName) {
  var docs = [];
  for (var i = j; i < x + j; i++) {
      docs.push({
          name: 'Tim' + i,
          surname: 'Richards' + i,
          //password: crypto.randomBytes(8),
          score: i, // index
          teacher: 'Bob',
          city: 'Cape Town',
          age: Math.floor((Math.random() * 100) + 1),
          iq: Math.floor((Math.random() * 2000) - 1000)
          //obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
      });
  };
  console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
  console.time(':: Insert Time');
  db[cName].insert(docs);
  console.timeEnd(':: Insert Time');
  console.log('<DB> Records:', db[cName].count());
  console.log('<DB> Records (loaded):', db[cName].loaded());
}
