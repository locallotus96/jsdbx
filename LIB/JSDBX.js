/* Adapted with respect for:
 *
 * jsonfs
 *
 * Copyright (c) 2014 Jalal Hejazi
 * Licensed under the MIT license.
 */

'use strict';
// Global Modules
var path = require('path');
var mkdirp = require('mkdirp');

// Local Modules
var FILER = require('./FILER.js');

var msg = {
    connect_success: '<DB> Successfully connected to DB: ',
    connect_error_db_path: '<DB> The DB Path "%s" is not valid. Creating path...',
    connect_error_db_cname: '<DB> The Collection Name "%s" is not valid. It must be a string with no special characters',
    loadCollection_initialize: '<DB> Initialize the DB before you work with collections. Use: db.connect("path-to-db","collection")'
};

/* NOTE
   We can create one exposed DB object, that can connect to different paths and collections at a time.
   We need to disconnect from collections when done using them for safety.
   Each connection to a collection corresponds to a new DAL object being created,
   this is the db engine instance working with the collection.
*/

/* NOTE: By creating new _db objects each time through path,
   we can use a single DB object to connect to multiple DAL objects (Each DAL object c),
   and access simply by performing db.path.collection calls.
*/

var db = {
    connect: function(path, collection, callback) {
        console.error('<DB> Connecting to:', path + '/' + collection);
        // if this db object already has a property set to the desired collection object (DAL)
        if(this[collection]) {
            console.error('<DB> Already connected to:', path + '/' + collection);
            callback('<DB> Already connected to: ' + path + '/' + collection);
            return;
        }
        if (FILER.isValidPath(path)) {
            var _db = {}; // create a new DB object
            _db.path = path; // set it's path name to that of supplied format <'DB'> or <'DB/myCollections'> path name
            this._db = _db; // attach it to this exposed db object as a new property
        } else { // The DB with supplied path does not exist but we can create it
            console.log(msg.connect_error_db_path, path);
            mkdirp.sync(path); // create the directory for us
            var _db = {}; // create a new DB object
            _db.path = path; // set it's path name to that of supplied format <'DB'> or <'DB/myCollections'> path name
            this._db = _db; // attach it to this exposed db object as a property
        }
        // if we supplied a valid collection name string
        if (collection && typeof(collection) === 'string') {
            console.error(msg.connect_success + path);
            this.loadCollections(collection, callback);
        } else {
            console.error(msg.connect_error_db_cname + path);
            callback(msg.connect_error_db_cname + path);
            return;
        }
        return;
    },
    disconnect: function(collection, callback) {
        if(!this[collection]) {
            console.log(msg.loadCollection_initialize);
            console.log('<DB> Cannot disconnect from unknown collection!');
            callback('Cannot disconnect from unknown collection!');
        } else {
            console.log('<DB> Saving collection before disconnecting...');
            this[collection].save(function(err) { // call DAL.save()
                if(err) {
                    console.error('<DB> Error saving collection!', err);
                } else {
                  console.log('<DB> Collection saved! Clearing memory and disconnecting...');
                  db[collection] = undefined; // delete DAL object corresponding to this collection
                  //db._db = undefined;
                }
                callback(err);
            });

        }
    },
    loadCollections: function(collection, callback) {
        if (!this._db) {
            console.log(msg.loadCollection_initialize);
            callback(msg.loadCollection_initialize);
        }
        if (typeof collection === 'string' && collection.length > 0) {
            // NOTE
            // Here we convert the supplied collection path/cname string to that of the corresponding path/file name, as used by the rest of the program,
            // in order to check if the file exists.
            var p = path.join(this._db.path, (collection.indexOf('.db') >= 0 ? collection : collection + '.db'));
            if (!FILER.isValidPath(p)) {
                console.log('<DB> Collection does not exist! Creating...');
                FILER.resetFileSync(p);
            }
            // convert the collection name string back to that which the user supplied
            var _c = collection.replace('.db', '');
            // NOTE: Our DB object (this) gets a property 'path' which has collection name, set to a new DAL object,
            // and we pass in the file name we created earlier, as well as the collection name itself
            // Doing all this means we can connect to multiple collections via different DB.DAL objects
            this[_c] = new require('./DAL.js')(path.join(this._db.path, (_c + '.db')), _c);

            console.log('<DB> Loading Collection:', _c);
            // NOTE
            // DAL.load() automatically starts the loading process, and loads in a partition (file) of records into memory (eg: up to 1 or 2 GB from a single file depending on settings)
            // The program will automatically load more as needed
            this[_c].load(function(err) {
                if(err) {
                    console.error('<DB> Error Loading Collection!', err);
                } else {
                    console.log('<DB> Finished Loading Collection:', _c + ' ', db[_c].loaded() + ' records');
                }
                callback(err);
            });
        } else {
            console.log('<DB> Invalid Collection Name String!');
        }
    }
};

module.exports = db; // return this DB object back to caller
