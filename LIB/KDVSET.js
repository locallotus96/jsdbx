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
    this.COUNT = 0; // no. of unique keys/properties
    this.DEEP_COUNT = 0; // no. of values in all keys

    this.add = function (key, val) {
        if(this.SET[key]) { // key exists
            this.SET[key].push(val);
            this.DEEP_COUNT++;
        } else { // new key
            this.SET[key] = [val];
            this.COUNT++;
            this.DEEP_COUNT++;
        }
        return true;
    }

    this.get = function (key) {
        return this.SET[key];
    }

    // Proceed with caution...
    this.update = function (oldKey, newKey, val, remove) {
        if(this.contains(oldKey)) { // check that the old key exists
            // Now we need to check if the key contains a pointer to the object
            // get the array of values from the key
            var p = this.get(oldKey);
            // check if object block index is in the array
            // We could loop backwards so we don't loop in the same direction as we're splicing
            // Otherwise we miss half the references as splice() modifies the array in place
            //console.log(':: KDVSET.update oldkey|newkey|val|values:', oldKey, newKey, val, p.length);
            //for(var i = p.length-1; i >= 0; i--) {
            for(var i = 0; i < p.length; i++) {
                // check if blki at i equals blki in value
                if(p[i] === val) {
                    // ok there exists a key with the same block index

                    // remove value with splice
                    p.splice(i, 1);
                    this.DEEP_COUNT--;
                    /* The array is being re-indexed when you do a .splice(),
                       which means you'll skip over an index when one is removed, and your cached .length is obsolete.
                       To fix it, you'd either need to decrement i after a .splice(), or simply iterate in reverse...
                       This way the re-indexing doesn't affect the next item in the iteration,
                       since the indexing affects only the items from the current point to the end of the Array,
                       and the next item in the iteration is lower than the current point. */
                    i--; // decerement index so we don't skip it,

                    if(!remove) { // we aim to update
                        //console.log(':: KDVSET.update Adding: oldkey|newkey|val|values:', oldKey, newKey, val, p.length);
                        // put back the old key and value
                        this.add(oldKey, val);
                        //console.log(this.get(newKey));
                    }
                    // if this key contains no more values, remove it
                    if(p.length === 0) {
                        console.log(':: KDVSET.update Removing empty key:', oldKey, p);
                        this.remove(oldKey);
                    }

                    return true; // NOTE We only do one at a time, since we come back for each indexed value in the old key so we can update the index
                }
            }
        } else { // oldKey isn't even here, just insert newKey if updating
            if(!remove) { // we're not here to delete this missing key :)
                // insert the new key and value
                console.log(':: KDVSET.update (!oldKey) Adding new key:', newKey, val);
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
        //this.SET = {};
        delete this.SET;
        this.COUNT = 0;
        this.DEEP_COUNT = 0;
    }

    this.size = function () {
        return Object.keys(this.SET).length;
    }

    this.count = function () {
        return this.COUNT;
    }

    this.deepCount = function () {
        return this.DEEP_COUNT;
    }
}
