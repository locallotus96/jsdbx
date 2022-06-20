'use strict';
var KDVSET = require('./KDVSET');

/*
    The indexer currently stores an object of keys/values for each indexed field.
    Values for each key is an array of block indices, thus pointing to the doc with this key
    Duplicate keys have their corresponding values aka block indices pushed into the values array.

    TODO: Values could be of type kvset (since we don't expect keys to reference a doc/block index more than once)
*/

var INDEXER = {};

INDEXER.INDICES = {}; // indexed fields and their k/v set objects
INDEXER.INDEXED = 0; // no. of indices
// NOTE Not used / implemented yet
// The DB should reject inserts with indexed fields that exceed the key limit
// Updates should follow the same restriction
INDEXER.KEY_LIMIT = 1024; // bytes, fields with values (aka keys) larger than this should not be indexed
INDEXER.MAX_INDICES = 32; // max no. of indexed fields per collection

// iteratively build an index on the supplied field
INDEXER.build = function (field, collection) {
    console.time('INDEXER - Build Time on ' + field);
    if(typeof(field) === 'object' && field.length) {
        var f = '';
        for(var i = 0; i < field.length; i++) {
            f = field[i];
            if(!this.INDICES[f]) {
                this.INDICES[f] = new KDVSET(); // Get a new index object for this field
                this.INDEXED++;
            }
        }
        for(var i = 0; i < collection.length; i++) {
            for(var j = 0; j < field.length; j++) {
                f = field[j];
                this.INDICES[f].add(collection[i][f], collection[i]._blki);
            }
        }
    } else {
        if(!this.INDICES[field]) {
            this.INDICES[field] = new KDVSET(); // Get a new index object for this field
            this.INDEXED++;
        }
        for(var i = 0; i < collection.length; i++) {
            this.INDICES[field].add(collection[i][field], collection[i]._blki);
        }
    }
    console.timeEnd('INDEXER - Build Time on ' + field);
}

INDEXER.add = function (field, key, val) {
    return this.INDICES[field].add(key, val); // add a key and value to this index key
}

INDEXER.get = function (field, key) {
    if(this.INDICES[field])
        return this.INDICES[field].get(key); // get a key and value from this indexed key
    else
        return [];
}

 // updates to the index valalue need to happen on deletes, inserts and updates to the db,
 // for any new or changed fields in the val that have an index
INDEXER.update = function (field, oldKey, newKey, val, remove) {
    return this.INDICES[field].update(oldKey, newKey, val, remove); // update a key and value
}

INDEXER.destroy = function(field) {
    if(typeof(field) === 'object' && field.length) {
        var f = '';
        var removed = 0;
        for(var i = 0; i < field.length; i++) {
            f = field[i];
            if(this.INDICES[f]) {
                this.INDICES[f].clear();
                this.INDEXED--;
                delete this.INDICES[f]; // delete the reference to our index set
                removed++;
            }
        }
        return removed;
    } else {
        if(this.INDICES[field]) {
            this.INDICES[field].clear();
            this.INDEXED--;
            delete this.INDICES[field]; // delete the reference to our index set
            return true;
        } else {
            return false;
        }
    }
}

INDEXER.getNoIndices = function() {
    return this.INDEXED;
}

INDEXER.getIndices = function() {
    var indices = [];
    for(var p in this.INDICES)
        indices.push(p)
    return indices;
}

INDEXER.getSize = function(field) {
    if(typeof(field) === 'object' && field.length) {
        var f = '';
        var c = 0;
        for(var i = 0; i < field.length; i++) {
            f = field[i];
            if(this.INDICES[f]) {
                c += this.INDICES[f].count(); // .size() itterates over the index valalue's keys, .count() returns a counter value
            }
        }
        return c;
    } else {
        if(this.INDICES[field]) {
            return this.INDICES[field].count(); // .size() itterates over the index valalue's keys, .count() returns a counter value
        } else {
            return -1;
        }
    }
}

INDEXER.deepSize = function(field) {
    if(typeof(field) === 'object' && field.length) {
        var f = '';
        var c = 0;
        for(var i = 0; i < field.length; i++) {
            f = field[i];
            if(this.INDICES[f]) {
                c += this.INDICES[f].deepCount(); // .size() itterates over the index valalue's keys, .count() returns a counter value
            }
        }
        return c;
    } else {
        if(this.INDICES[field]) {
            return this.INDICES[field].deepCount(); // .size() itterates over the index valalue's keys, .count() returns a counter value
        } else {
            return -1;
        }
    }
}

// Allows using new INDEXER()
module.exports = function() {
    return INDEXER;
}
//module.exports = INDEXER;
