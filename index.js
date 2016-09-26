/**
 * @module couchbaselib
 */
'use strict';

const
	couchbase   = require('couchbase'),
	CouchBaseEnvironment  = require('./lib/environment')(couchbase);


module.exports = new CouchBaseEnvironment();