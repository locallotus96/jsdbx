# JavaScript DBX
## (WIP) A little JSON database in Node.js

> Supports basic queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field
  - Select specific fields to be included in return documents
  - Limit no. returned documents to x
  - Skip x no. of documents to be returned
  - Sort returned documents by a specific field

> Supports indexing on almost any field(s) for faster searches - See Notes section!
  - Indices are dynamically maintained

> Supports connections to multiple collections through a single db object

> This is primarily an in-memory database at the moment, only saves on disconnect!

### Dependencies

> (https://www.npmjs.com/package/mkdirp "mkdirp") by (https://github.com/substack "substack")
> (https://www.npmjs.com/package/merge "merge") by (https://github.com/yeikos "yeikos")
> (https://www.npmjs.com/package/node-uuid "node-uuid") by (https://github.com/broofa "broofa")

You can run: <?!#$> npm install to install the dependencies in package.json

#### Inspiration
> (https://www.npmjs.com/package/json_file_system "json_file_system") by (https://github.com/jalalhejazi/jsonfs "jalalhejazi")
> The NoSQL ecosystem and how databases work

### API:

#### Loading the module and connecting to a collection

> var db = require('./LIB/JSDBX.js')

path is the path from the base directory to the collection  
collection is the name of the collection to connect to  
callback takes an err parameter to signal successful connection/disconnection or failure thereof  
> db.connect(path, collection, callback)  
> db.disconnect(path, collection, callback)

#### Creating / Removing indices
key is a string of the field/property name to index on  
> db.collection.createIndex(key)  
> db.collection.removeIndex(key)  

#### Inserting data
data can be a single object or an array of objects
> db.collection.insert(data)  

#### Finding
query is an object with keys/values to search for  
> db.collection.find(query)  
> db.collection.findOne(query)  
> db.collection.findAny(query)  
> db.collection.findAnyOne(query)  

#### Updating
data is an object of keys/values to merge with a matching object  
> db.collection.update(query, data)  
> db.collection.updateOne(query, data)  
> db.collection.updateAny(query, data)  
> db.collection.updateAnyOne(query, data)  

#### Removing
> db.collection.remove(query)  
> db.collection.removeOne(query)  
> db.collection.removeAny(query)  
> db.collection.removeAnyOne(query)  

#### Total records
> db.collection.count()  

#### Don't call these methods from db.collection
###### (mind the other exposed vars as well)
used by db.connect()  
> db.collection.load(callback)  

used by db.disconnect()  
> db.collection.save(callback)  

### Notes:
> Do not index on fields that contain objects (aka embedded documents)!
> But you can safely embed objects in your doc, you just can't search on the embedded object (yet).
> You need to disconnect to save the database to file!
> I'm working on 'live' persistence through the file system...
> Indices have to be explicitly rebuilt after connecting again

### Architecture:
> Where <=> contains methods somehow exposed to the user via quantum something or other
> <=> JSDBX  
>     :: <=> new DAL() => new PERSISTENCE() => new INDEXER() => new KDVSET()  

### Future Plans
I plan to get this running in it's own server process(s) with a client library you can use to connect to it.
I'd also like to port it to Python, or parts of it and see how it performs.

And finally, i'm just building this for fun.
