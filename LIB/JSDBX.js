/* Adapted with respect for:
 *
 * jsonfs
 *
 * Copyright (c) 2014 Jalal Hejazi
 * Licensed under the MIT license.
 */

'use strict';
//global modules
var path = require('path');
var mkdirp = require('mkdirp');

//local modules
var UTIL = new(require('./UTIL.js'));
var msg = {
    connect_success: 'Successfully connected to : ',
    connect_error_db_path:'The DB Path "%s" is not valid. Creating path...',
    loadCollection_initialize:'Initialize the DB before you add collections. Use: db.connect(path-to-db,["collection"])'
};

var db = {
    connect: function(path, collection, callback) {
        if(this[collection]) {
            console.error('Already connected to:', collection);
            return;
        }
        if (UTIL.isValidPath(path)) {
            var _db = {};
            _db.path = path;
            this._db = _db;
            console.log(msg.connect_success + path);
            if (collection) {
                this.loadCollections(collection, callback);
            }
        } else {
            console.log(msg.connect_error_db_path, path);
            mkdirp.sync(path); // create the directory for us
            var _db = {};
            _db.path = path;
            this._db = _db;
            console.log(msg.connect_success + path);
            if (collection) {
                this.loadCollections(collection, callback);
            }
        }
        return this;
    },
    disconnect: function(collection, callback) {
        if (!this._db) {
            console.log(msg.loadCollections_initialize);
            callback(false);
        }
        if(!this[collection]) {
            console.log('Cannot disconnect from unknown collection!');
            callback(false);
        } else {
            console.log('Saving collection before disconnecting...');
            this[collection].save(function(err) {
                if(err) {
                    console.error('Error saving collection', err);
                    callback(err);
                    throw err;
                }
                console.log('Collection saved! Clearing memory and disconnecting...');
                db[collection] = undefined;
                db._db = undefined;
                callback();
            });

        }
        return true;
    },
    loadCollections: function(collection, callback) {
        if (!this._db) {
            console.log(msg.loadCollection_initialize);
            return false;
        }
        if (typeof collection === 'string' && collection.length > 0) {
            var p = path.join(this._db.path, (collection.indexOf('.db') >= 0 ? collection : collection + '.db'));
            if (!UTIL.isValidPath(p)) {
                console.log('Collection does not exist! Creating...');
                UTIL.resetFileSync(p);
            }
            var _c = collection.replace('.db', '');
            this[_c] = new require('./DAL.js')(this, _c);
            console.log('Loading Collection:', _c);
            this[_c].load(function(err) {
                if(err) {
                    console.error('Error Loading Collection!');
                } else {
                    console.log('Finished Loading and Inserting Collection:', _c + ' ', db[_c].count() + ' records');
                }
                callback(err);
            });
        } else {
            console.log('Invalid Collection Name String!');
        }
        return this;
    }
};

module.exports = db;
