'use strict';
/*
  An unordered collection of unique elements built for fast membership testing, insertion and removal.

  Note: Python has built-in hash-table types, dict and set.
        Keys in dicts must be immutable.
        Sets are as dictionaries without values.

  In javascript objects must have a key and value, so we set the value it's pointing at to null.
  Note:
    undefined is for variables or properties that do not exist or have not yet been assigned a value
    null represents the intentional absence of any value.

  Usage:
    var SET = require('./SET.js');
    var myset = new SET();
    myset.add(key);

  API:
    SET.add(key)
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

    this.add = function (key) {
        if(this.SET[key]) { // key exists
            return false;
        } else { // new key
            this.SET[key] = null;
            this.COUNT++;
            return true;
        }
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
