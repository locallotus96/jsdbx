# JavaScript DBX
## A simple mockup of a database in nodejs.

> Supports indexing on any field(s) for fast searches
  - Indices are dynamically maintained
  - Indices yield roughly O(1) access time on any combination of fields (indexed vs normal)  

> Supports queries on multiple fields:  
  - Find One  
  - Find Many  
  -- Match on all fields  
  -- Match on any field  

### API:

db = require('./LIB/JSDBX.js')

db.connect(collection, callback)  
db.disconnect(collection, callback)

db.collection => load(callback)  
db.collection => save(callback)  
db.collection ===> createIndex(key)  
db.collection ===> removeIndex(key)  
db.collection =====> insert(data)  
db.collection =======> find(query)  
db.collection =======> findOne(query)  
db.collection =======> findAny(query)  
db.collection =======> findAnyOne(query)  
db.collection =========> update(query, data)  
db.collection =========> updateOne(query, data)  
db.collection =========> updateAny(query, data)  
db.collection =========> updateAnyOne(query, data)  
db.collection ===========> remove(query)  
db.collection ===========> removeOne(query)  
db.collection ===========> removeAny(query)  
db.collection ===========> removeAnyOne(query)  
db.collection =============> count()  

### Architecture:

<=> JSDBX
  :: => new UTIL
  :: <=> new DAL(UTIL) => UTIL => INDEXER => new KDVSET   
