var couchbase = require('couchbase'),
    async = require('async'),
    N1qlQuery = require('couchbase').N1qlQuery;
var _ = require('lodash');


var internals = {
    host: null,
    adminPor: null,
    apiPort: null,
    sslAdminPort: null,
    sslApiPort: null,
    cluster: null,
    buckets: [],
    defaultPassword: null,
    n1qlPort: 8093,
    viewQueryOptions: {
        stale: couchbase.ViewQuery.Update.NONE,
        skip: undefined,
        limit: undefined,
        order: undefined,
        reduce: undefined,
        group: undefined,
        group_level: undefined,
        key: undefined,
        keys: undefined,
        range: undefined,   //{start:0, end:10, inclusive_end:true} Specifies a range of keys to retrieve from the index.
        id_range: undefined, //{start:0, end:10} Specifies a range of document id's to retrieve from the index.
        include_docs: undefined, //Flag to request a view request include the full document value.
        full_set: undefined //Flag to request a view request accross all nodes in the case of
    },
    db: {}
};

var CouchBaseDB = function () {

};

CouchBaseDB.prototype.initConnection = function (host, password, adminPort, apiPort, sslAdminPort, sslApiPort) {
    internals.host = host;
    internals.adminPort = adminPort;
    internals.apiPort = apiPort || 8092;
    internals.sslAdminPort = sslAdminPort || 18091;
    internals.sslApiPort = sslApiPort || 18092;
    internals.defaultPassword = password;
    if (adminPort)
        internals.cluster = new couchbase.Cluster(host + ':' + adminPort);
    else
        internals.cluster = new couchbase.Cluster(host);

};

CouchBaseDB.prototype.upsert = function (bucketName, docName, newDoc, resultCallback, options) {

    if (!options || options && options.constructor !== Object) {
        options = {
            retrieve: !!options
        };
    }

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.upsert(docName, newDoc, callback);
        }
    ], function (err, results) {
        if (options.retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};

CouchBaseDB.prototype.get = internals.get = function (bucketName, docName, options, resultCallback) {

    if (resultCallback === void 0) {
        resultCallback = options;
        options = {};
    }

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.get(docName, options, callback);
        }
    ], function (err, results) {
        resultCallback(err, internals.couchResultToJSON(results));
    });
};

CouchBaseDB.prototype.getAndLock = internals.getAndLock = function (bucketName, docName, options, resultCallback) {
    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.getAndLock(docName, options, callback);
        }
    ], function (err, results) {
        resultCallback(err, internals.couchResultToJSON(results));
    });
};

CouchBaseDB.prototype.getN1qlQuery = internals.getN1qlQuery = function (bucketName, query, resultCallback) {
    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            var _query = N1qlQuery.fromString(query);
            bucket.query(_query, callback);
        }
    ], function (err, results) {
        resultCallback(err, internals.couchResultToJSONSql(results, bucketName));
    });
};

CouchBaseDB.prototype.insert = function (bucketName, docName, doc, resultCallback, options) {

    if (!options || options && options.constructor !== Object) {
        options = {
            retrieve: !!options
        };
    }

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.insert(docName, doc, callback);
        }
    ], function (err, results) {
        if (options.retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};

CouchBaseDB.prototype.insertWithIndexingViews = function (bucketName, docName, doc, resultCallback, options /*, views*/) {
    var _query = this.query;

    if (!options || options && options.constructor !== Object) {
        options = {
            retrieve: !!options,
            views: arguments[5]
        };
    }
    options.views = options.views || options; // compatibility with older signature

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.insert(docName, doc, callback);
        },
        function (data, callback) {
            if (options.views) {
                var arr = [];

                if (Array.isArray(options.views)) {
                    for (var i = 0; i < options.views.length; i++) {
                        var view = options.views[i];
                        var obj = function (cl) {
                            view.options = view.options || {};
                            _.defaultsDeep(view.options, internals.viewQueryOptions);
                            view.options.stale = couchbase.ViewQuery.Update.BEFORE;
                            _query(view.bucketName, view.design, view.viewQuery, cl, view.options)
                        };
                        arr.push(obj);
                    }
                }
                else {
                    var obj = function (cl) {
                        options.views.options = options.views.options || {};
                        _.defaultsDeep(options.views.options, internals.viewQueryOptions);
                        options.views.options.stale = couchbase.ViewQuery.Update.BEFORE;
                        _query(options.views.bucketName, options.views.design, options.views.viewQuery, cl, options.views.options)
                    };
                    arr.push(obj);
                }

                async.parallel(arr, function (err, results) {
                    callback(err, results);
                });

            } else {
                callback(null, data);
            }

        }
    ], function (err, results) {
        if (options.retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};


CouchBaseDB.prototype.remove = function (bucketName, docName, resultCallback) {
    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.remove(docName, callback);
        }
    ], function (err, results) {
        resultCallback(err, results);
    });
};

CouchBaseDB.prototype.removeWithIndexingViews = function (bucketName, docName, resultCallback, options) {
    var _query = this.query;

    if (!options || options && options.constructor !== Object) {
        options = {
            views: []
        };
    }
    options.views = options.views || options; // compatibility with old

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.remove(docName, callback);
        },
        function (data, callback) {
            if (options.views) {
                var arr = [];
                if (Array.isArray(options.views)) {
                    for (var i = 0; i < options.views.length; i++) {
                        var view = options.views[i];
                        var obj = function (cl) {
                            view.options = view.options || {};
                            _.defaultsDeep(view.options, internals.viewQueryOptions);
                            view.options.stale = couchbase.ViewQuery.Update.BEFORE;
                            _query(view.bucketName, view.design, view.viewQuery, cl, view.options)
                        };
                        arr.push(obj);
                    }
                }
                else {
                    var obj = function (cl) {
                        options.views.options = options.views.options || {};
                        _.defaultsDeep(options.views.options, internals.viewQueryOptions);
                        options.views.options.stale = couchbase.ViewQuery.Update.BEFORE;
                        _query(options.views.bucketName, options.views.design, options.views.viewQuery, cl, options.views.options)
                    };
                    arr.push(obj);
                }

                async.parallel(arr, function (err, results) {
                    callback(err, results);
                });

            } else {
                callback(null, data);
            }

        }
    ], function (err, results) {
        resultCallback(err, results);
    });
};

CouchBaseDB.prototype.update = function (bucketName, docName, newDoc, resultCallback, options) {

    if (!options || options && options.constructor !== Object) {
        options = {
            retrieve: !!options
        };
    }

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.replace(docName, newDoc, options, callback);
        }
    ], function (err, results) {
        if (options.retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};

CouchBaseDB.prototype.updateWithIndexingViews = function (bucketName, docName, newDoc, resultCallback, options/*, views*/) {

    var

        _query = this.query;

    if (!options || options && options.constructor !== Object) {
        options = {
            retrieve: !!options,
            views: arguments[5]
        };
    }
    options.views = options.views || views; // compatibility with older signature

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.replace(docName, newDoc, options, callback);
        },
        function (data, callback) {
            if (options.views) {
                var arr = [];
                if (Array.isArray(options.views)) {
                    for (var i = 0; i < options.views.length; i++) {
                        var view = options.views[i];
                        var obj = function (cl) {
                            view.options = view.options || {};
                            _.defaultsDeep(view.options, internals.viewQueryOptions);
                            view.options.stale = couchbase.ViewQuery.Update.BEFORE;
                            _query(view.bucketName, view.design, view.viewQuery, cl, view.options)
                        };
                        arr.push(obj);
                    }
                }
                else {
                    var obj = function (cl) {
                        options.views.options = options.views.options || {};
                        _.defaultsDeep(options.views.options, internals.viewQueryOptions);
                        options.views.options.stale = couchbase.ViewQuery.Update.BEFORE;
                        _query(options.views.bucketName, options.views.design, options.views.viewQuery, cl, options.views.options)
                    };
                    arr.push(obj);
                }

                async.parallel(arr, function (err, results) {
                    callback(err, results);
                });

            } else {
                callback(null, data);
            }

        }
    ], function (err, results) {
        if (options.retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};


CouchBaseDB.prototype.counter = function (bucketName, docName, delta, resultCallback, retrieve) {
    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.counter(docName, delta, {initial: 0}, callback);
        }
    ], function (err, results) {
        if (retrieve) {
            internals.get(bucketName, docName, resultCallback);
        } else {
            resultCallback(err, results);
        }
    });
};

CouchBaseDB.prototype.unlock = function unlock(bucketName, docName, cas, options, callback) {

    if (callback === void 0) {
        callback = options;
        options = {};
    }
    options = options || {};

    async.waterfall([
        function (callback) {
            internals.getBucket(bucketName, callback);
        },
        function (bucket, callback) {
            bucket.unlock(docName, cas, callback);
        }
    ], function (err, results) {
        callback(err, internals.couchResultToJSON(results));
    });
};

function getMulti(bucketName, keys, resultCallback) {
    if (!keys || keys.length === 0) {
        resultCallback(null, []);
    } else {
        async.waterfall([
            function (callback) {
                internals.getBucket(bucketName, callback);
            },
            function (bucket, callback) {
                bucket.getMulti(keys, callback);
            }
        ], function (err, results) {
            if (err && err > 0) {
                return resultCallback(results);
            }
            return resultCallback(null, internals.couchMultiResultToJSON(results));
        });
    }

}
CouchBaseDB.prototype.getMulti = getMulti;


function query(bucketName, design, viewQuery, resultCallback, options, populate) {
    try {
        var _options = _.pick(_.defaults(options || internals.viewQueryOptions, internals.viewQueryOptions), _.keys(internals.viewQueryOptions));
        var _populate = populate || false;
        var _getMulti = this.getMulti;

        async.waterfall([
            function (callback) {
                internals.getBucket(bucketName, callback);
            },
            function (bucket, callback) {
                var ViewQuery = couchbase.ViewQuery;
                var query = ViewQuery.from(design, viewQuery).stale(_options.stale);

                if (_options.limit !== undefined) {
                    query.limit(_options.limit);
                }

                if (_options.skip !== undefined) {
                    query.skip(_options.skip);
                }
                if (_options.order !== undefined) {
                    query.order(_options.order);
                }
                if (_options.reduce !== undefined) {
                    query.reduce(_options.reduce);
                }
                if (_options.group !== undefined) {
                    query.group(_options.group);
                }
                if (_options.group_level !== undefined) {
                    query.group_level(_options.group_level);
                }
                if (_options.key !== undefined) {
                    query.key(_options.key);
                }
                if (_options.keys !== undefined) {
                    query.keys(_options.keys);
                }
                if (_options.range !== undefined) {
                    query.range(_options.range.start, _options.range.end, _options.range.inclusive_end);
                }
                if (_options.id_range !== undefined) {
                    query.id_range(_options.range.start, _options.range.end);
                }
                if (_options.include_docs !== undefined) {
                    query.include_docs(_options.include_docs);
                }
                if (_options.full_set !== undefined) {
                    query.include_docs(_options.full_set);
                }

                bucket.query(query, callback);
            }
        ], function (err, results) {
            if (err) {
                resultCallback(err.message);
            }
            else if (!_populate) {
                resultCallback(err, results);
            } else {
                var count = results.length;
                var keys = [];
                for (var i = 0; i < count; i++) {
                    keys.push(results[i].id);
                }
                _getMulti(bucketName, keys, resultCallback);
            }

        });
    } catch (e) {
        resultCallback(e);
    }
}
CouchBaseDB.prototype.query = query;

internals.getConnection = function () {
    if (internals.instance === null)
        internals.initConnection(internals.host, internals.port);
    return internals.cluster;
};

internals.getBucket = function (name, password, callback) {
    if (typeof(password) === 'function') {
        callback = arguments[1];
        password = internals.defaultPassword;
    }

    if (typeof(internals.buckets[name]) === 'undefined') {
        var _bucket = internals.getConnection().openBucket(name, password, function (err) {
            if (err) {
                return callback(err);
            } else {
                internals.buckets[name] = _bucket;
                _bucket.enableN1ql('http://' + internals.host + ':' + internals.n1qlPort);
                return callback(undefined, _bucket);
            }
        });
    } else {
        return callback(undefined, internals.buckets[name]);
    }
};

CouchBaseDB.prototype.getBucket = internals.getBucket;

internals.couchResultToJSON = function (couchResult) {
    if (couchResult) {
        if (typeof(couchResult.value) !== "undefined") {
            return couchResult.value;
        }
        else if (typeof(couchResult) === "object") {
            var newObject = {};
            for (var key in couchResult) {
                if (couchResult.hasOwnProperty(key)) {
                    newObject[key] = internals.couchResultToJSON(couchResult[key]);
                }
            }
            return newObject;

        } else if (Array.isArray(couchResult)) {
            var newArray = [];
            for (var result in couchResult) {
                newArray.push(internals.couchResultToJSON(couchResult[result]));
            }
            return newArray;
        }
    }

};


internals.couchResultToJSONSql = function (couchResult, bucket) {
    if (couchResult) {
        var newArray = [];
        for (var result in couchResult) {
            newArray.push(couchResult[result][bucket]);
        }
        return newArray;

    }
};

internals.couchMultiResultToJSON = function (couchResult) {
    if (couchResult) {
        if (typeof(couchResult.value) !== "undefined") {
            var obj = {value: couchResult.value};
            return obj;
        }
        else if (typeof(couchResult) === "object") {
            var newObject = [];
            for (var key in couchResult) {
                if (couchResult.hasOwnProperty(key)) {
                    newObject.push(internals.couchMultiResultToJSON(couchResult[key]));
                }
            }
            return newObject;

        } else if (Array.isArray(couchResult)) {
            var newArray = [];
            for (var result in couchResult) {
                newArray.push(internals.couchMultiResultToJSON(couchResult[result]));
            }
            return newArray;
        }
    }

};


exports = module.exports = new CouchBaseDB;