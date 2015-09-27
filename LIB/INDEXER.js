'use strict';
var KDVSET = require('./KDVSET');

var INDEXER = {};

INDEXER.INDECES = {};

INDEXER.build = function (field, collection) {
    var KDV_SET = new KDVSET();
    this.INDECES[field] = KDV_SET;
    for(var i = 0; i < collection.length; i++) {
        KDV_SET.add(collection[i][field], collection[i]);
    }
}

INDEXER.add = function (field, obj) {
    return this.INDECES[field].add(obj[field], obj);
}

INDEXER.get = function (field, key) {
    return this.INDECES[field].get(key);
}

// TODO: Put update for removal in own function
INDEXER.update = function (field, oldKey, newKey, obj, remove) {
    return this.INDECES[field].update(oldKey, newKey, obj, remove);
}

INDEXER.destroy = function(field) {
    if(this.INDECES[field]) {
        this.INDECES[field].clear();
        delete this.INDECES[field]; // delete the reference to our index set
        return true;
    } else {
        return false;
    }
}

module.exports = INDEXER;
