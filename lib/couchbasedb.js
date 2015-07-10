var couchbase = require('couchbase'),
	async = require('async'),
	N1qlQuery = require('couchbase').N1qlQuery;
	

var internals = {
	host:null,
	port:null,
	cluster:null,
	buckets:[],
	n1qlPort:8093,
	viewQueryOptions:{
		stale: couchbase.ViewQuery.Update.NONE,
		skip : null,
		limit : null,
		order : null,
		reduce : null,
		group : null,
		group_level  : null,
		key : null,
		keys : null,
		range : null,   //{start:0, end:10, inclusive_end:true} Specifies a range of keys to retrieve from the index.
		id_range : null, //{start:0, end:10} Specifies a range of document id's to retrieve from the index.
		include_docs : null, //Flag to request a view request include the full document value.
		full_set : null //Flag to request a view request accross all nodes in the case of
	},
	db:{}
};

var CouchBaseDB= function(){

};

CouchBaseDB.prototype.initConnection = function(host, port){
	internals.host = host;
	internals.port = port;
	if(port)
		internals.cluster = new couchbase.Cluster('couchbase://'+host+':'+port);
	else
		internals.cluster = new couchbase.Cluster('couchbase://'+host);
		
};

CouchBaseDB.prototype.upsert = function(bucketName, docName, newDoc, resultCallback, retrieve) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.upsert(docName, newDoc, callback);
		}	
	], function(err, results) {
		if(retrieve){
			internals.get(bucketName, docName, resultCallback);
		}else{
			resultCallback(err, results);
		}	
	});
};

CouchBaseDB.prototype.get = internals.get = function(bucketName, docName, resultCallback ) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.get(docName, callback);
		}	
	], function(err, results) {
		resultCallback(err, internals.couchResultToJSON(results));
	});
};

CouchBaseDB.prototype.getN1qlQuery = internals.getN1qlQuery = function(bucketName, query, resultCallback ) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			var _query = N1qlQuery.fromString(query);
			bucket.query(_query, callback);
		}	
	], function(err, results) {
		resultCallback(err, internals.couchResultToJSONSql(results, bucketName));
	});
};

CouchBaseDB.prototype.insert = function(bucketName, docName, doc, resultCallback, retrieve, views) {
	var _query = this.query;
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.insert(docName, doc, callback);
		}
	], function(err, results) {
		if(retrieve){
			internals.get(bucketName, docName, resultCallback);
		}else{
			resultCallback(err, results);
		}
	});
};

CouchBaseDB.prototype.inserteWithIndexingViews = function(bucketName, docName, doc, resultCallback, retrieve, views) {
	var _query = this.query;
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.insert(docName, doc, callback);
		},
		function(data, callback){
			if(views){
				var arr = [];
				for(var i=0; i<views.length; i++){
					var view = views[i];
					var obj = function(cl){
						if(!view.options){
							view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							view.options.limit=0;
						}else{
							if(!view.options.stale){
								view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							}
							if(!view.options.limit){
								view.options.limit=0;
							}
						}

						_query(view.bucketName, view.design, view.viewQuery, cl, view.options)
					}
					arr.push(obj);
				}

				async.parallel(arr, function(err, results){
					callback(err, results);
				});

			}else{
				callback(null, data);
			}

		}
	], function(err, results) {
		if(retrieve){
			internals.get(bucketName, docName, resultCallback);
		}else{
			resultCallback(err, results);
		}		
	});
};

CouchBaseDB.prototype.getMulti = function(bucketName, docNames, resultCallback) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.getMulti(docNames, callback);
		}	
	], function(err, results) {
		resultCallback(err, results);
	});
};

CouchBaseDB.prototype.remove = function(bucketName, docName, resultCallback) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.remove(docName, callback);
		}	
	], function(err, results) {
		resultCallback(err, results);
	});
};

CouchBaseDB.prototype.removeWithIndexingViews = function(bucketName, docName, resultCallback, views) {
	var _query = this.query;
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.remove(docName, callback);
		},
		function(data, callback){
			if(views){
				var arr = [];
				for(var i=0; i<views.length; i++){
					var view = views[i];
					var obj = function(cl){
						if(!view.options){
							view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							view.options.limit=0;
						}else{
							if(!view.options.stale){
								view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							}
							if(!view.options.limit){
								view.options.limit=0;
							}
						}

						_query(view.bucketName, view.design, view.viewQuery, cl, view.options)
					}
					arr.push(obj);
				}

				async.parallel(arr, function(err, results){
					callback(err, results);
				});

			}else{
				callback(null, data);
			}

		}
	], function(err, results) {
		resultCallback(err, results);
	});
};

CouchBaseDB.prototype.update = function(bucketName, docName, newDoc, resultCallback, retrieve) {
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.replace(docName, newDoc, callback);
		}	
	], function(err, results) {
		if(retrieve){
			internals.get(bucketName, docName, resultCallback);
		}else{
			resultCallback(err, results);
		}	
	});
};


CouchBaseDB.prototype.updateWithIndexingViews = function(bucketName, docName, newDoc, resultCallback, retrieve, views) {
	var _query = this.query;
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.replace(docName, newDoc, callback);
		},
		function(data, callback){
			if(views){
				var arr = [];
				for(var i=0; i<views.length; i++){
					var view = views[i];
					var obj = function(cl){
						if(!view.options){
							view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							view.options.limit=0;
						}else{
							if(!view.options.stale){
								view.options.stale=couchbase.ViewQuery.Update.BEFORE;
							}
							if(!view.options.limit){
								view.options.limit=0;
							}
						}

						_query(view.bucketName, view.design, view.viewQuery, cl, view.options)
					}
					arr.push(obj);
				}

				async.parallel(arr, function(err, results){
					callback(err, results);
				});

			}else{
				callback(null, data);
			}

		}
	], function(err, results) {
		if(retrieve){
			internals.get(bucketName, docName, resultCallback);
		}else{
			resultCallback(err, results);
		}
	});
};

function query(bucketName, design, viewQuery, resultCallback, options) {
	
	var _options = internals.viewQueryOptions;
	
	if(options){
		_options = options;
		if(!_options.stale){
			_options.stale = internals.viewQueryOptions.stale;
		}
	}
	
	async.waterfall([
		function(callback) {
			internals.getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			var ViewQuery = couchbase.ViewQuery;
			var query = ViewQuery.from(design, viewQuery).stale(_options.stale);
			
			if(_options.limit!==undefined){
				query.limit(_options.limit);
			}

			if(_options.skip!==undefined){
				query.skip(_options.skip);
			}
			if(_options.order!==undefined){
				query.order(_options.order);
			}
			if(_options.reduce!==undefined){
				query.reduce(_options.reduce);
			}
			if(_options.group!==undefined){
				query.group(_options.group);
			}
			if(_options.group_level!==undefined){
				query.group_level(_options.group_level);
			}
			if(_options.key!==undefined){
				query.key(_options.key);
			}
			if(_options.keys!==undefined){
				query.keys(_options.keys);
			}
			if(_options.range!==undefined){
				query.range(_options.range.start, _options.range.end, _options.range.inclusive_end);
			}
			if(_options.id_range!==undefined ){
				query.id_range(_options.range.start, _options.range.end);
			}
			if(_options.include_docs!==undefined){
				query.include_docs(_options.include_docs);
			}
			if(_options.full_set!==undefined){
				query.include_docs(_options.full_set);
			}
			
			bucket.query(query, callback);
		}	
	], function(err, results) {
		resultCallback(err, results);
	});
};

CouchBaseDB.prototype.query = query;

internals.getConnection = function(){
    if(internals.instance === null)
        internals.initConnection(internals.host, internals.port);
    return internals.cluster;
};

internals.getBucket = function(name, callback){
	if(typeof(internals.buckets[name])==='undefined'){		
		var _bucket = internals.getConnection().openBucket(name, function(err) {
			if(err) {
				callback(err);
			} else {		
				internals.buckets[name] = _bucket;
				_bucket.enableN1ql('http://'+internals.host+':'+internals.n1qlPort);
				callback(undefined, _bucket);
			}
		});
	}else{
		callback(undefined, internals.buckets[name]);
	}
};

CouchBaseDB.prototype.getBucket = internals.getBucket;

internals.couchResultToJSON = function( couchResult ) {
	if(couchResult){
		if(typeof(couchResult.value) !== "undefined" ) {
			return couchResult.value;
		}
		else if( typeof(couchResult) === "object") {
			var newObject = {};
			for(var key in couchResult) {
				if(couchResult.hasOwnProperty(key)) {
					newObject[key] = internals.couchResultToJSON(couchResult[key]);
				}
			}
			return newObject;
	
		} else if( Array.isArray(couchResult) ) {
			var newArray = [];
			for(var result in couchResult) {
				newArray.push( internals.couchResultToJSON(couchResult[result]));
			}
			return newArray;
		}
	}
	
};


internals.couchResultToJSONSql = function( couchResult, bucket ) {
	if(couchResult){
			var newArray = [];
			for(var result in couchResult) {
				newArray.push( couchResult[result][bucket]);
			}
			return newArray;
		
	}
};


exports = module.exports = new CouchBaseDB;