'use strict';

/*
  Caching module, will cache references to recently accessed objects
*/

var CACHER = {};

CACHER.CACHE = [];

CACHER.insert = function (obj) {
    this.CACHE.push(obj);
}

module.exports = CACHER;
