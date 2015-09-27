# JavaScript DBX
## A json object database in node.js

> Supports indexing on any field(s) for fast searches
  - Indices are dynamically maintained

> Supports queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field  

I hit max records at just over 1750000 (1.75 mil) records given my test insertion.  
This safely saved 253MB of data from a json stringified array of objects.  
JSON.stringify and JSON.parse can't deal with more data on my machine.  

### API:

#### Loading the module and connecting to a collection

> var db = require('./LIB/JSDBX.js')

path is path to the collection(s) and collection is the name of the specific collection to connect  
callback takes an err parameter to signal successful connection/disconnection or failure thereof  
> db.connect(path, collection, callback)  
> db.disconnect(path, collection, callback)

#### Don't call these methods from db.collection unless you're hacking
###### (mind the other exposed vars as well)
used by db.connect()  
> db.collection => load(callback)  

used by db.disconnect()  
> db.collection => save(callback)  

#### Creating indices
key is a string of the field/property name to index on  
> db.collection ===> createIndex(key)  
> db.collection ===> removeIndex(key)  

#### Inserting data
data can be a single object or an array of objects
> db.collection =====> insert(data)  

#### Finding
query is an object with keys/values to search for  
> db.collection =======> find(query)  
> db.collection =======> findOne(query)  
> db.collection =======> findAny(query)  
> db.collection =======> findAnyOne(query)  

#### Updating
data is an object of keys/values to merge with a matching object  
> db.collection =========> update(query, data)  
> db.collection =========> updateOne(query, data)  
> db.collection =========> updateAny(query, data)  
> db.collection =========> updateAnyOne(query, data)  

#### Removing
> db.collection ===========> remove(query)  
> db.collection ===========> removeOne(query)  
> db.collection ===========> removeAny(query)  
> db.collection ===========> removeAnyOne(query)  

#### Total records
> db.collection =============> count()  

### Architecture:
> <=> JSDBX  
>     :: => new UTIL  
>     :: <=> new DAL(UTIL) => UTIL => INDEXER => new KDVSET  
