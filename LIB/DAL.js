'use strict';
// Global Modules
var path = require('path');

//--- DATA ACCESS LAYER API

module.exports = function(file, collectionName) {
  var DAL = {}; // data access layer object (class), gets exported

  // PUBLIC VARIABLES
  DAL.CNAME = collectionName;
  DAL.FILE = file;

  /* PRIVATE VARIABLES (should not be exported except for testing) */
  var PERSISTENCE = new require('./PERSISTENCE.js')();
  DAL.LOADING = false;
  DAL.SAVING = false;

  DAL.load = function (callback) {
      if(!this.LOADING) {
          this.LOADING = true;
          PERSISTENCE.loadCollection(this.FILE, function(err) {
              DAL.LOADING = false;
              callback(err);
          });
      } else { // We're busy loading
          callback(false);
      }
  }

  DAL.save = function (callback) {
      if(!this.SAVING) {
          this.SAVING = true;
          PERSISTENCE.saveCollection(this.FILE, PERSISTENCE.COLLECTION, function(err) {
              DAL.SAVING = false;
              callback(err);
          });
      } else { // We're busy saving
          callback(false);
      }
  }

  DAL.createIndex = function (field) {
      if(PERSISTENCE.createIndex(field, PERSISTENCE.COLLECTION)) {
          return true;
      } else {
          return false;
      }
  }

  DAL.removeIndex = function (field) {
      if(PERSISTENCE.destroyIndex(field)) {
          return true;
      } else {
          return false;
      }
  }

  DAL.indices = function () {
      return PERSISTENCE.getIndices();
  }

  DAL.indicesCount = function () {
      return PERSISTENCE.getNoIndices();
  }

  DAL.indexSize = function (field) {
      return PERSISTENCE.indexSize(field);
  }

  DAL.indexDeepSize = function (field) {
      return PERSISTENCE.indexDeepSize(field);
  }

  DAL.count = function () {
      return PERSISTENCE.getTotalDocs();
  }

  DAL.loaded = function () {
      return PERSISTENCE.getLoadedDocs();
  }

  /*
    data can be an object or an array of objects (1D or 2D array)
  */
  DAL.insert = function (data) {
      if(typeof data !== 'object') {
          return 0; // invalid data
      }
      return PERSISTENCE.inserter(PERSISTENCE.COLLECTION, data);
  }

  DAL.findOne = function (query, options) {
      if(!query) {
          return [];
      }
      return (PERSISTENCE.finder(PERSISTENCE.COLLECTION, query, false, true, options))[0] || {};
  }

  DAL.findAnyOne = function (query, options) {
      if(!query) {
          return [];
      }
      return (PERSISTENCE.finder(PERSISTENCE.COLLECTION, query, false, false, options))[0] || {};
  }

  DAL.find = function (query, options) {
      if(!query) {
          return PERSISTENCE.COLLECTION.slice(); // return a copy
      }
      return PERSISTENCE.finder(PERSISTENCE.COLLECTION, query, true, true, options);
  }

  DAL.findAny = function (query, options) {
      if(!query) {
          return PERSISTENCE.COLLECTION.slice(); // return a copy
      }
      return PERSISTENCE.finder(PERSISTENCE.COLLECTION, query, true, false, options);
  }

  DAL.updateOne = function (query, data) {
      if(!query || !data) {
          return 0;
      }
      return PERSISTENCE.updater(PERSISTENCE.COLLECTION, query, data, false, true);
  }

  DAL.updateAnyOne = function (query, data) {
      if(!query || !data) {
          return 0;
      }
      return PERSISTENCE.updater(PERSISTENCE.COLLECTION, query, data, false, false);
  }

  DAL.update = function (query, data) {
      if(!query || !data) {
          return 0;
      }
      return PERSISTENCE.updater(PERSISTENCE.COLLECTION, query, data, true, true);
  }

  DAL.updateAny = function (query, data) {
      if(!query || !data) {
          return 0;
      }
      return PERSISTENCE.updater(PERSISTENCE.COLLECTION, query, data, true, false);
      return updated;
  }

  DAL.removeOne = function (query) {
      if(!query) {
          return 0;
      }
      return PERSISTENCE.remover(PERSISTENCE.COLLECTION, query, false, true);
  }

  DAL.removeAnyOne = function (query) {
      if(!query) {
          return 0;
      }
      return PERSISTENCE.remover(PERSISTENCE.COLLECTION, query, false, false);
  }

  DAL.remove = function (query) {
      if(!query) {
          return 0;
      }
      return PERSISTENCE.remover(PERSISTENCE.COLLECTION, query, true, true);
  }

  DAL.removeAny = function (query) {
      if(!query) {
          return 0;
      }
      return PERSISTENCE.remover(PERSISTENCE.COLLECTION, query, true, false);
  }

  return DAL;
}
