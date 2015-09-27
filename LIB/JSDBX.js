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
var UTIL = require('./UTIL.js');
var msg = {
    connect_success: 'Successfully connected to : ',
    connect_error_db_path:'The DB Path "%s" is not valid. Creating path...',
    loadCollections_initialize:'Initialize the DB before you add collections. Use: db.connect(path-to-db,["collection"])'
};

var db = {
    connect: function(path, collections) {
        if (UTIL.isValidPath(path)) {
            var _db = {};
            _db.path = path;
            this._db = _db;
            console.log(msg.connect_success + path);
            if (collections) {
                this.loadCollections(collections);
            }
        } else {
            console.log(msg.connect_error_db_path, path);
            mkdirp.sync(path); // create the directory for us
            var _db = {};
            _db.path = path;
            this._db = _db;
            console.log(msg.connect_success + path);
            if (collections) {
                this.loadCollections(collections);
            }
            //return false;
        }
        return this;
    },
    disconnect: function(collection) {
        if (!this._db) {
            console.log(msg.loadCollections_initialize);
            return false;
        }
        if(!this[collection]) {
            console.log('Cannot disconnect from unknown collection!');
            return false;
        } else {
            console.log('Saving collection before disconnecting...');
            this[collection].save(function(err) {
                if(err) {
                    console.error('Error saving collection', err);
                    throw err;
                }
                console.log('Collection saved! Clearing memory and disconnecting...');
                db[collection] = undefined;
                db._db = undefined;
            });

        }
        return true;
    },
    loadCollections: function(collections) {
        if (!this._db) {
            console.log(msg.loadCollections_initialize);
            return false;
        }
        if (typeof collections === 'object' && collections.length) {
            for (var i = 0; i < collections.length; i++) {
                var p = path.join(this._db.path, (collections[i].indexOf('.db') >= 0 ? collections[i] : collections[i] + '.db'));
                if (!UTIL.isValidPath(p)) {
                    console.log('Collection does not exist! Creating...');
                    UTIL.resetFileSync(p);
                }
                var _c = collections[i].replace('.db', '');
                this[_c] = new require('./DAL.js')(this, _c);
                this[_c].load(function() {
                    console.log('Finished Loading Collection:', _c + ' ', db[_c].count() + ' records');
                });
            }
        } else {
            console.log('Invalid Collections Array.', 'Expected Format : ', '[\'collection1\',\'collection2\',\'collection3\']');
        }
        return this;
    }

};

module.exports = db;
