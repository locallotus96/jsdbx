# JavaScript DBX
## (WIP) Another little JSON database in Node.js

> Supports basic queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field
  - Select specific fields to be included in returned document(s)
  - Limit no. of returned documents
  - Skip x no. of documents to be returned
  - Sort returned documents by a specific field

> Supports indexing on any comparible === field(s) for faster searches (collection is held as an array, indexes as K/V pairs)
  - Indices are dynamically maintained (See Notes section)

> You can connect to multiple collections using a single db object

> And this is primarily an in-memory database at the moment, it only saves on disconnect

### Dependencies

> (https://www.npmjs.com/package/mkdirp "mkdirp") by (https://github.com/substack "substack")
> (https://www.npmjs.com/package/merge "merge") by (https://github.com/yeikos "yeikos")
> (https://www.npmjs.com/package/node-uuid "node-uuid") by (https://github.com/broofa "broofa")

You can run:
> ?!#$ npm install

to install the dependencies specified in package.json

#### Inspiration
> My fellow coder Tyrell
> (https://www.npmjs.com/package/json_file_system "json_file_system") by (https://github.com/jalalhejazi/jsonfs "jalalhejazi")
> The NoSQL ecosystem and how databases work

### API:

#### Loading the module and connecting to a collection

> var db = require('./LIB/JSDBX.js')

path is the path from the base directory to the collection  
collection is the name of the collection to connect to  
callback takes an err parameter to signal successful connection / disconnection
> db.connect(path, collection, callback)  
> db.disconnect(path, collection, callback)

Example:
> db.connect('localfolder/mydb', 'mycollection', function(err) {
>     if(err) { // something happened
>         console.log(err); // what about middleware?
>     } else { // success
>          myqueryfunctions(); // next(); // middleware?
>     }
> })

#### Creating / Removing indices
key is a string of the field/property name to index on  
> db.collection.createIndex(key)  
> db.collection.removeIndex(key)  

Example:
> db.test.createIndex('name');

#### Inserting data
data can be a single object or an array of objects
> db.collection.insert(data)  

#### Finding
query is an object with keys/values to search for  
options is an object with any combination of these fields: select, limit, skip and sort
- select takes a string or an array of strings, containing the names of fields to return
- limit takes a number to limit the number of returned records by
- skip takes a number spcifyinh how many found records to skip when returning
- sort takes a string name of the field to sort records by
> db.collection.find(query, <options>)  // find matching all supplied fields:values
> db.collection.findOne(query, <options>)  
> db.collection.findAny(query, <options>)  // find matching any supplied field:value
> db.collection.findAnyOne(query, <options>)  

Examples:
> db.test.find({ name: 'Mike' });
> db.test.findOne({ age:46 }, {select:'name'});
> db.test.findAny({ age:20 }, {select:['name', 'age' ], limit:5, skip:15, sort:'age'});

#### Updating
data is an object of keys/values to merge with a matching object  
> db.collection.update(query, data)  
> db.collection.updateOne(query, data)  
> db.collection.updateAny(query, data)  
> db.collection.updateAnyOne(query, data)  

Examples:
> db.test.update({ name:'Mike' }, {name:'Foo'});
> db.test.updateOne({ age:46 }, {name:'Bar'});
> db.test.updateAny({ age:20, name:'Bar' }, {city: 'Istanbul'});

#### Removing
> db.collection.remove(query)  
> db.collection.removeOne(query)  
> db.collection.removeAny(query)  
> db.collection.removeAnyOne(query)  

Examples:
> db.test.remove({ city:'Cape Town' });
> db.test.removeOne({ age:46 });
> db.test.removeAny({ name:'Brian', age:30 });

#### Total records
> db.collection.count()  

#### Don't call these methods from db.collection
###### (mind the other exposed vars as well)
used by db.connect()  
> db.collection.load(callback)  

used by db.disconnect()  
> db.collection.save(callback)  

### Notes:
> Don't try and index on fields that contain objects (aka embedded documents)
> However, you can safely embed objects in your doc, you just can't search on the embedded object (yet).
> You need to disconnect to save the database to file
> I'm working on a more 'lively' persistence method through the file system...
> Indices have to be explicitly rebuilt after connecting again, for now...

### Architecture:
> Where <=> contains methods exposed to the user as the API
> <=> JSDBX  
>     :: <=> new DAL() => new PERSISTENCE() => new INDEXER() => new KDVSET()  

And finally, i'm just building this for fun, to better understand javascript.
