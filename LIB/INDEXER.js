'use strict';
var KDVSET = require('./KDVSET');

var INDEXER = {};

INDEXER.INDICES = {};

INDEXER.build = function (field, collection) {
    this.INDICES[field] = new KDVSET();
    for(var i = 0; i < collection.length; i++) {
        this.INDICES[field].add(collection[i][field], collection[i]);
    }
}

INDEXER.add = function (field, obj) {
    return this.INDICES[field].add(obj[field], obj);
}

INDEXER.get = function (field, key) {
    return this.INDICES[field].get(key);
}

INDEXER.update = function (field, oldKey, newKey, obj, remove) {
    return this.INDICES[field].update(oldKey, newKey, obj, remove);
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
