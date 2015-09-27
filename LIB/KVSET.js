'use strict';
/*
  Key Value Store
  A key/value store implementation
  No duplicates are allowed

  Note: Python has built-in hash-table types, dict and set.
        Keys in dicts must be immutable.
        Sets are as dictionaries without values.

  Usage:
    var KVSET = require('./KVSET.js');
    var mykvset = new KVSET();
    mykvset.add(key, value);

  API:
    SET.add(key,val)
    SET.get(key)
    SET.update(key,val)
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
            return false;
        } else { // new key
            this.SET[key] = val;
            this.COUNT++;
        }
        return true;
    }

    this.get = function (key) {
        return this.SET[key];
    }

    this.update = function (key, val) {
        if(this.SET[key]) {
            this.SET[key] = val;
            return true;
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
