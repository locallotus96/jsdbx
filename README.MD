# JavaScript DBX
## A little (WIP) json object database in node.js

> Supports basic queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field  

> Supports indexing on any field(s) for faster searches
  - Indices are dynamically maintained

> Supports connections to multiple collections

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

### Architecture:
> <=> JSDBX  
>     :: <=> new DAL() => new PERSISTENCE() => new INDEXER() => new KDVSET()  

### Future Plans
I plan to get this running in it's own server process(s) with a client library you can use to connect to it.
I'd also like to port it to Python, or parts of it and see how it performs.
Querying could probably be improved / expanded but this is JavaScript afterall...
