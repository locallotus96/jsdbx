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

DAL => UTIL => INDEXER => KDVSET   

### API:
=> load(callback)  
=> save()  
==> createIndex(key)  
==> removeIndex(key)  
===> insert(data)  
=====> find(query)  
=====> findOne(query)  
=====> findAny(query)  
=====> findAnyOne(query)  
=======> update(query, data)  
=======> updateOne(query, data)  
=======> updateAny(query, data)  
=======> updateAnyOne(query, data)  
=====> remove(query)  
=====> removeOne(query)  
=====> removeAny(query)  
=====> removeAnyOne(query)  
===> count()  
