var Schema = require('./schema');
var Model = require('./model');
var _ = require('lodash');
var CouchBaseDB = require('./couchbasedb');

var internals = {
	models:{},
	couchbase:null
};


function Couch(){
	internals.couchbase = CouchBaseDB;
	this.Schema = Schema;
};

Couch.prototype.initConnection = function(host, port){
	internals.couchbase.initConnection(host, port);
};

Couch.prototype.getBucket = function(name, cb){
	internals.couchbase.getBucket(name, cb);
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
	Model.DB = internals.couchbase;
	Model.bucket = internals.models[name].bucket;
	_.pick(_.defaults(Model, internals.models[name].methods), _.keys(internals.models[name].methods)); 
	return Model;
};

Couch.prototype.upsert = CouchBaseDB.prototype.upsert;
Couch.prototype.get = CouchBaseDB.prototype.get;
Couch.prototype.getN1qlQuery = CouchBaseDB.prototype.getN1qlQuery;
Couch.prototype.insert = CouchBaseDB.prototype.insert;
Couch.prototype.getMulti = CouchBaseDB.prototype.getMulti;
Couch.prototype.remove = CouchBaseDB.prototype.remove;
Couch.prototype.query = CouchBaseDB.prototype.query;
Couch.prototype.update = CouchBaseDB.prototype.update;


exports = module.exports = new Couch();