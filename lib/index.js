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

	Instance.findById = function (id, cb) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.findById(Instance.bucket, x, cb);
	};

	Instance.findByIdAndLock = function (id, cb) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.findByIdAndLock(Instance.bucket, x, cb);
	};

	Instance.update = function (id, data, cb, options) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.update(x, data, Instance.bucket, Instance.schema, cb, options);
	};

	Instance.unlockAndUpdate = function (id, data, cas, cb, noDeep) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.unlockAndUpdate(x, data, cas, Instance.bucket, Instance.schema, cb, noDeep);
	};

	Instance.updateWithIndexingViews = function (id, data, cb, views, noDeep) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.updateWithIndexingViews(x, data, Instance.bucket, Instance.schema, cb, views, noDeep);
	};

	Instance.unlockAndUpdateWithIndexingViews = function (id, data, cas, cb, views, noDeep) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.unlockAndUpdateWithIndexingViews(x, data, cas, Instance.bucket, Instance.schema, cb, views, noDeep);
	};

	Instance.remove = function (id, cb) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.remove(x, Instance.bucket, cb);
	};

	Instance.removeWithIndexingViews = function (id, cb, views) {
		var x = Model.modelKey(Instance.type, id);
		this.super_.removeWithIndexingViews(x, Instance.bucket, cb, views);
	};

	Instance.counter = function (id, delta, cb){
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
