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
};

Couch.prototype.initConnection = function(host, port){
	this.db.initConnection(host, port);
};

Couch.prototype.getBucket = function(name, cb){
	this.db.getBucket(name, cb);
};

Couch.prototype.model = function(name, schema, bucket, methods){

	var inject = {
		name: name,
		schema: schema,
		bucket: bucket,
		methods: methods,
		_type:name.toLowerCase()
	}

	internals.models[name]=inject;
};

Couch.prototype.get = function(name){		
	Model.DB = this.db;
	var rs = internals.models[name];

	function Instance(data) {
		this.data = data;
		this.data._type = rs._type;
		this.schema = rs.schema;
		this.key = rs._type;
		this.bucket = rs.bucket;
		_.pick(_.defaults(Instance, rs.methods), _.keys(rs.methods));
	}

	util.inherits(Instance, Model);

	Instance.findById = function(bucket, type, key, cb){
		var x = Model.modelKey(type, key);
		this.super_.findById(bucket, x, cb);
	}


	return Instance;
};

exports = module.exports = new Couch();