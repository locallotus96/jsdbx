# JavaScript DBX
## A simple mockup of a database in nodejs.

> Supports indexing on any field(s) (dynamically maintained)
> Supports queries on multiple fields:
  - Find One
  - Find Many
  -- Match on all fields
  -- Match on any field

### API:
:: load(callback)
:: save()
:::: createIndex(key)
:::: removeIndex(key)
:: insert(data)
:::: find(query)
:::: findOne(query)
:::: findAny(query)
:::: findAnyOne(query)
:: update(query, data)
:: updateOne(query, data)
:: updateAny(query, data)
:: updateAnyOne(query, data)
:::: remove(query)
:::: removeOne(query)
:::: removeAny(query)
:::: removeAnyOne(query)
:: count()

### Info Links:
https://blog.glyphobet.net/essay/2557
https://github.com/nzakas/computer-science-in-javascript/
http://codewinds.com/blog/2013-08-19-nodejs-writable-streams.html
http://stackoverflow.com/questions/9486683/writing-large-files-with-node-js
https://github.com/BonsaiDen/BiSON.js/
