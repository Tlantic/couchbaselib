var Schema = require('./schema');
var Model = require('./model');
var _ = require('lodash');
var CouchBaseDB = require('./couchbasedb');
var util = require('util');

var internals = {
	models:{},
	couchbase:null
};


function Couch(){
	this.db = CouchBaseDB;
	this.Schema = Schema;
}

Couch.prototype.initConnection = function(host, password, adminPort, apiPort, sslAdminPort, sslApiPort){
	this.db.initConnection(host, password, adminPort, apiPort, sslAdminPort, sslApiPort);
};

Couch.prototype.getBucket = function(name, password, cb){
	this.db.getBucket(name, password, cb);
};

Couch.prototype.model = function(name, schema, bucket, methods){

	var inject = {
		name: name,
		schema: schema,
		bucket: bucket,
		methods: methods,
		_type:schema.key
	};

	internals.models[name]=inject;
};

Couch.prototype.get = function(name) {
	Model.DB = this.db;
	var rs = internals.models[name];

	function Instance(data) {
		this.data = data;
		this.data._type = rs._type;
		this.schema = rs.schema;
		this.key = rs._type;
		this.bucket = rs.bucket;
	}

	_.pick(_.defaults(Instance, rs.methods), _.keys(rs.methods));

	util.inherits(Instance, Model);

	Instance.type = rs._type;
	Instance.bucket = rs.bucket;
	Instance.schema = rs.schema;

	/**
	 * Retrieves a document by its id
	 * @param id
	 * @param options
	 * @param cb
	 */
	Instance.findById = function (id, options, cb) {

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}

		var x = Model.modelKey(Instance.type, id);
		this.super_.findById(Instance.bucket, x, options, cb);
	};

	/**
	 * Retrieves and locks a document by its id
	 * @param id
	 * @param options
	 * @param cb
	 */
	Instance.findByIdAndLock = function (id, options, cb) {

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}
		
		var x = Model.modelKey(Instance.type, id);
		this.super_.findByIdAndLock(Instance.bucket, x, options, cb);
	};

	Instance.update = function (id, data, options, cb) {

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}

		var x = Model.modelKey(Instance.type, id);
		this.super_.update(x, data, Instance.bucket, Instance.schema, options, cb);
	};

	Instance.updateWithIndexingViews = function (id, data, views, options, cb) {/*id, data, cb, views, noDeep*/

		if ( typeof arguments[2] === 'function' ) {
			views = arguments[3];
			options = {
				noDeep: arguments[4]
			};
			cb = views;
		}

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}


		var x = Model.modelKey(Instance.type, id);
		this.super_.updateWithIndexingViews(x, data, Instance.bucket, Instance.schema, views, options, cb);
	};

	Instance.remove = function (id, options, cb) {

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}

		var x = Model.modelKey(Instance.type, id);
		this.super_.remove(x, Instance.bucket, options, cb);
	};

	Instance.removeWithIndexingViews = function (id, views, options, cb) { /*id, cb, views*/

		if ( typeof arguments[1] === 'function' ) {
			cb = arguments[1];
			views = arguments[2];
			options = {};
		}

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}

		var x = Model.modelKey(Instance.type, id);
		this.super_.removeWithIndexingViews(x, Instance.bucket, views, options, cb);
	};

	Instance.counter = function (id, delta, cb){

		if ( cb === void 0 ) {
			cb = options;
			options = {};
		}

		var x = Model.modelKey(Instance.type, id);
		this.super_.counter(Instance.bucket, x, delta, cb);
	};

	Instance.getMulti = function (keys, cb) {
		var count = keys.length;
		var _keys = [];
		for (var i = 0; i < count; i++) {
			_keys.push(Model.modelKey(Instance.type, keys[i]));
		}
		this.super_.getMulti(Instance.bucket, _keys, cb);
	};
	
	return Instance;
};

exports = module.exports = new Couch();
