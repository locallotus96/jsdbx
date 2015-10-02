'use strict';
var KDVSET = require('./KDVSET');

var INDEXER = {};

INDEXER.INDICES = {};

// iteratively build an index on the supplied field
INDEXER.build = function (field, collection) {
    this.INDICES[field] = new KDVSET(); // Get a new index object for this field
    for(var i = 0; i < collection.length; i++) {
        this.INDICES[field].add(collection[i][field], collection[i]);
    }
}

INDEXER.add = function (field, obj) {
    return this.INDICES[field].add(obj[field], obj); // add a key and value to this index object
}

INDEXER.get = function (field, key) {
    return this.INDICES[field].get(key); // get a key and value from this index object
}

 // updates to the index object need to happen on deletes, inserts and updates to the db,
 // for any new or changed fields in the obj that have an index
INDEXER.update = function (field, oldKey, newKey, obj, remove) {
    return this.INDICES[field].update(oldKey, newKey, obj, remove); // update a key and value
}

INDEXER.destroy = function(field) {
    if(this.INDICES[field]) {
        this.INDICES[field].clear();
        delete this.INDICES[field]; // delete the reference to our index set
        return true;
    } else {
        return false;
    }
}

// Allows using new INDEXER()
module.exports = function() {
    return INDEXER;
}
//module.exports = INDEXER;
