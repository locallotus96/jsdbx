# JavaScript DBX
## A json object database in node.js

> Supports indexing on any field(s) for fast searches
  - Indices are dynamically maintained
  - Indices yield roughly O(1) access time on any combination of fields (proportional to indexed vs normal)  

> Supports queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field  

I hit max records at just over 1750000 (1.75 mil) records given my test insertion.
This safely saved 253MB of a json stringified array of objects.
JSON.stringify and JSON.parse can't deal with more data on my machine.

### API:

> db = require('./LIB/JSDBX.js')

path is path to the collection(s) and collection is the name of the specific collection  
callback takes an err parameter to signal successful connection/disconnection or failure thereof  
> db.connect(path, collection, callback)  
> db.disconnect(path, collection, callback)

used by db.connect()  
> db.collection => load(callback)  

used by db.disconnect()  
> db.collection => save(callback)  

key is a string of field/property name to index on  
> db.collection ===> createIndex(key)  
> db.collection ===> removeIndex(key)  

data can be a single object or an array of objects
> db.collection =====> insert(data)  

query is an object with keys/values to search for  
> db.collection =======> find(query)  
> db.collection =======> findOne(query)  
> db.collection =======> findAny(query)  
> db.collection =======> findAnyOne(query)  

data is an object of keys/values to merge with a matching object  
> db.collection =========> update(query, data)  
> db.collection =========> updateOne(query, data)  
> db.collection =========> updateAny(query, data)  
> db.collection =========> updateAnyOne(query, data)  
> db.collection ===========> remove(query)  
> db.collection ===========> removeOne(query)  
> db.collection ===========> removeAny(query)  
> db.collection ===========> removeAnyOne(query)  
> db.collection =============> count()  

### Architecture:

> <=> JSDBX  
>     :: => new UTIL  
>     :: <=> new DAL(UTIL) => UTIL => INDEXER => new KDVSET  
