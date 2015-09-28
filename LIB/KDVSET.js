'use strict';
/*
  Key Duplicate Value Store
  A key/value store implementation where values are stored in an array
  and the values of duplicate keys are added to the value array of the existing key.

  Note: Python has built-in hash-table types, dict and set.
        Keys in dicts must be immutable.
        Sets are as dictionaries without values.

  Usage:
    var KDVSET = require('./KDVSET.js');
    var mykdvset = new KDVSET();
    mykdvset.add(key, value);

  API:
    SET.add(key,val)
    SET.get(key)
    SET.update(oldKey,newKey,val,remove)
    SET.remove(key)
    SET.rename(oldKey,newKey)
    SET.contains(key)
    SET.clear()
    SET.size()
    SET.count()
*/

module.exports = function () {
    this.SET = {}; // the key/value store
    this.COUNT = 0; // number of keys/properties

    this.add = function (key, val) {
        if(this.SET[key]) { // key exists
            this.SET[key].push(val);
        } else { // new key
            this.SET[key] = [val];
            this.COUNT++;
        }
        return true;
    }

    this.get = function (key) {
        return this.SET[key];
    }

    // TODO: Put update for removal in own function
    // Proceed with caution...
    this.update = function (oldKey, newKey, val, remove) {
        if(this.contains(oldKey)) { // check that the old key exists
            if(this.contains(newKey)) { // the new key exists
                // Now we need to check if the key contains a pointer to the object
                // get the array of values from the key
                var p = this.get(oldKey);
                // check if object reference is in the array
                // loop backwards so we don't loop in the same direction as we're splicing
                // Otherwise we miss half the references as splice() modifies the array in place
                for(var i = p.length-1; i >= 0; i--) {
                    // check if reference equals reference at index
                    if(p[i] === val) {
                        // remove reference with splice
                        p.splice(i, 1);
                        // if this index contains no more values, remove it
                        if(p.length === 0) {
                            //console.log(':: KDVSET.update Removing empty key:', oldKey, p);
                            this.remove(oldKey);
                        }
                        if(!remove) { // we aim to update
                            //console.log(':: KDVSET.update Adding new key:', newKey, val);
                            // insert the new key and value => object reference
                            this.add(newKey, val);
                        }
                        return true;
                    }
                }
            } else { // the new key does not exist
                // Now we need to remove the matching object reference for the new key from the old key
                // get the array of values from the old key
                var p = this.get(oldKey);
                // check if object reference is in the array
                // loop backwards so we don't loop in the same direction as we're splicing
                // Otherwise we miss half the references as splice() modifies the array in place
                for(var i = p.length-1; i >= 0; i--) {
                    // check if reference equals reference at index
                    if(p[i] === val) {
                        // ok there exists a key with the same object reference
                        // remove reference with splice
                        p.splice(i, 1);
                        // if this key contains no more values, remove it
                        if(p.length === 0) {
                            //console.log(':: KDVSET.update Removing empty key!:', oldKey, p);
                            this.remove(oldKey);
                        }
                        if(!remove) { // we aim to update
                            //console.log(':: KDVSET.update Adding new key!', newKey, val);
                            // insert the new key and value => object reference
                            this.add(newKey, val);
                        }
                        return true;
                    }
                }
            }
        } else { // oldKey isn't even here, insert newKey if updating
            if(!remove) { // we're not here to delete this missing key
                // insert the new key and value => object reference
                this.add(newKey, val);
                return true;
            }
        }
        return false;
    }

    this.remove = function (key) {
        if(this.SET[key]) {
            delete this.SET[key];
            this.COUNT--;
            return true;
        }
        return false;
    }

    this.rename = function (oldKey, newKey) {
        if(this.SET[oldKey] && !this.SET[newKey]) {
            // copy old value to new key
            this.SET[newKey] = this[oldKey];
            // delete old key/value
            delete this.SET[oldKey];
            return true;
        }
        return false;
    }

    this.contains = function (key) {
        if(this.SET[key]) {
            return true;
        }
        return false;
    }

    this.clear = function () {
        this.SET = {};
        this.COUNT = 0;
    }

    this.size = function () {
        return Object.keys(this.SET).length;
    }

    this.count = function () {
        return this.COUNT;
    }
}
