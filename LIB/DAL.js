'use strict';

var path = require('path');
//var UTIL = new(require('./UTIL.js'));

module.exports = function(db, collectionName, UTIL) {
    var DAL = {}; // data access layer object (class)

    // PUBLIC VARIABLES
    DAL.C_NAME = collectionName;
    DAL.FILE = path.join(db._db.path, (collectionName + '.db'));

    /* PRIVATE VARIABLES (should not be exported except for testing) */
    DAL.LOADING = false;
    DAL.SAVING = false;
    DAL.COLLECTION = []; // documents / collection of javascript objects

    //--- DATA ACCESS LAYER API

    DAL.load = function (callback) {
        if(!this.LOADING) {
            this.LOADING = true;
            console.log('Loading File:', this.FILE);
            UTIL.loadCollection(this.FILE, function(err, data) {
                if(!err) {
                    console.log('Loaded Collection - Inserting records from file...');
                    //DAL.insert(data); // insert file data into the collection
                    if(data.length > 0 && typeof(data) === 'object') {
                        DAL.COLLECTION = data;
                    }
                    DAL.LOADING = false;
                }
                callback(err);
            });
        }
    }

    DAL.save = function (callback) {
        console.log('Saving:', this.count() + ' records');
        UTIL.saveCollection(this.FILE, this.COLLECTION, callback);
    }

    DAL.createIndex = function (field) {
        if(UTIL.createIndex(field, this.COLLECTION)) {
            return true;
        } else {
            return false;
        }
    }

    DAL.removeIndex = function (field) {
        if(UTIL.destroyIndex(field)) {
            return true;
        } else {
            return false;
        }
    }

    DAL.count = function () {
        return this.COLLECTION.length;
    }

    /*
      data can be an object or an array of objects (1D or 2D array)
    */
    DAL.insert = function (data) {
        if(typeof data !== 'object') {
            return 0; // invalid data
        }
        var inserted = UTIL.inserter(this.COLLECTION, data);
        return inserted;
    }

    DAL.findOne = function (query) {
        if(!query) {
            return [];
        }
        return (UTIL.finder(this.COLLECTION, query, false, true))[0];
    }

    DAL.findAnyOne = function (query) {
        if(!query) {
            return [];
        }
        return (UTIL.finder(this.COLLECTION, query, false, false))[0];
    }

    DAL.find = function (query) {
        if(!query) {
            return this.COLLECTION;
        }
        return UTIL.finder(this.COLLECTION, query, true, true);
    }

    DAL.findAny = function (query) {
        if(!query) {
            return this.COLLECTION;
        }
        return UTIL.finder(this.COLLECTION, query, true, false);
    }

    DAL.updateOne = function (query, data) {
        if(!query || !data) {
            return 0;
        }
        return UTIL.updater(this.COLLECTION, query, data, false, true);
    }

    DAL.updateAnyOne = function (query, data) {
        if(!query || !data) {
            return 0;
        }
        return UTIL.updater(this.COLLECTION, query, data, false, false);
    }

    DAL.update = function (query, data) {
        if(!query || !data) {
            return 0;
        }
        return UTIL.updater(this.COLLECTION, query, data, true, true);
    }

    DAL.updateAny = function (query, data) {
        if(!query || !data) {
            return 0;
        }
        return UTIL.updater(this.COLLECTION, query, data, true, false);
        return updated;
    }

    DAL.removeOne = function (query) {
        if(!query) {
            return 0;
        }
        return UTIL.remover(this.COLLECTION, query, false, true);
    }

    DAL.removeAnyOne = function (query) {
        if(!query) {
            return 0;
        }
        return UTIL.remover(this.COLLECTION, query, false, false);
    }

    DAL.remove = function (query) {
        if(!query) {
            return 0;
        }
        return UTIL.remover(this.COLLECTION, query, true, true);
    }

    DAL.removeAny = function (query) {
        if(!query) {
            return 0;
        }
        return UTIL.remover(this.COLLECTION, query, true, false);
    }

    return DAL;
}
