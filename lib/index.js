var Schema = require('./schema');
var Model = require('./model');
var _ = require('lodash');
var CouchBaseDB = require('./couchbasedb');

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
	Object.defineProperty(internals.models, name, {value:{
		name:name,
		schema:schema,
		methods: methods || {},
		bucket: bucket
	}});	
};

Couch.prototype.get = function(name){		
	Model.schema = internals.models[name].schema;
	Model.key = internals.models[name].schema.key;
	Model.DB = this.db;
	Model.bucket = internals.models[name].bucket;
	_.pick(_.defaults(Model, internals.models[name].methods), _.keys(internals.models[name].methods)); 
	return Model;
};

/*Couch.prototype.upsert = CouchBaseDB.upsert;
Couch.prototype.get = CouchBaseDB.get;
Couch.prototype.getN1qlQuery = CouchBaseDB.getN1qlQuery;
Couch.prototype.insert = CouchBaseDB.insert;
Couch.prototype.getMulti = CouchBaseDB.getMulti;
Couch.prototype.remove = CouchBaseDB.remove;
Couch.prototype.query = CouchBaseDB.query;
Couch.prototype.update = CouchBaseDB.update;*/

exports = module.exports = new Couch();