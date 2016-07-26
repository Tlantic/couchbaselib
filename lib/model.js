var _ = require('lodash');
var async = require('async');
var uuid = require('uuid');

var internals = {
    SYSTEM_USER: 'SYSTEM',
    lockedProperties: ['_uId', '_type', '_createDate', '_updateDate', '_createUser', '_updateUser']
};

function Model(data) {
    return;
}


Model.prototype.sanitize = function(data) {
    return data;
};

Model.prototype.schema;
Model.prototype.key;
Model.prototype.DB;
Model.prototype.bucket;

Model.prototype.createKey = function(id) {
    return this.key + '::' + id;
};

Model.modelKey = function(key, id) {
    return key + '::' + id;
};

/*
 * Models
 */

Model.findById = function(bucket, key, options, cb) {

    if (cb === void 0) {
        cb = options;
        options = {};
    }

    Model.DB.get(bucket, key, options, cb);
};

Model.findByIdAndLock = function(bucket, key, cb) {
    Model.DB.getAndLock(bucket, key, cb);
};

Model.getMulti = function(bucket, key, cb) {
    Model.DB.getMulti(bucket, key, cb);
};




Model.prototype.save = function(cb, options) {

    if (!options || options && options.constructor !== Object) {
        options = {};
    }
    options.retrieve = options.retrieve === "boolean" ? options.retrieve : true;


    this.data._uId = uuid.v4();
    this.data._createDate = this.data._updateDate =  Math.round(+new Date()/1000);
    this.data._createUser = this.data._updateUser =  options.updateUser || internals.SYSTEM_USER;

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.insert(this.bucket, this.createKey(this.data._uId), this.data, function(err, result) {
            if (err) {
                cb(err);
            } else {
                cb(null, result);
            }
        }, options);
    } else {
        cb(errors, null);
    }

};

Model.prototype.saveWithIndexingViews = function(cb, options) {

    if (!options) {
        options = {
            views: [],
            retrieve: true
        };
    } else if ( options.constructor !== Object) {
        options = {
            retrieve: true,
            views: []
        };
    } else if ( !options.views ) {
        options = {
            views: options,
            retrieve: typeof arguments[2] === "boolean" ? arguments[2] : true
        }
    }

    this.data._uId = uuid.v4();
    this.data._createDate = this.data._updateDate =  Math.round(+new Date()/1000);
    this.data._createUser = this.data._updateUser =  options.updateUser || internals.SYSTEM_USER;

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.insertWithIndexingViews(this.bucket, this.createKey(this.data._uId), this.data, function(err, result) {
            if (err) {
                cb(err);
            } else {
                cb(null, result);
            }
        }, options);
    } else {
        cb(errors, null);
    }

};


Model.update = function(id, data, bucket, schema, cb, options) {

    var _data = data;

    if (!options || options && options.constructor !== Object) {
        options = {};
    }
    options.retrieve = options.retrieve === "boolean" ? options.retrieve : true;


    async.waterfall([
        function(cb) {
            if ( options.cas ) {
                Model.unlock( bucket, id, options.cas, options, function( err) {
                    if ( err ) {
                        cb(err);
                    } else {
                        Model.findById(bucket, id, cb);
                    }
                } )
            } else {
                Model.findById(bucket, id, cb);
            }
        },
        function(data, cb) {
            _data = _.omit(_data, internals.lockedProperties);
            if(options.noDeep){
                _data = _.defaults(_data, data);
            }
            else{
                _data = _.defaultsDeep(_data, data);
            }
            _data._updateDate = Math.round(+new Date()/1000);
            _data._updateUser = options.updateUser || internals.SYSTEM_USER;

            var errors = schema.validate(_data);
            if(!errors) {
                Model.DB.update(bucket, id, _data, cb, options);
            }else{
                cb(errors, null);
            }
        }
    ], cb);

};

Model.updateWithIndexingViews = function(id, data, bucket, schema, cb, options) {

    var _data = data;

    if (!options) {
        options = {
            views: [],
            noDeep: false
        };
    } else if ( options.constructor !== Object) {
        options = {
            noDeep: options,
            views: []
        };
    } else if ( !options.views ) {
        options = {
            views: options,
            noDeep: typeof arguments[6] === "boolean" ? arguments[6] : true
        }
    }

    options.retrieve = options.retrieve || true;

    async.waterfall([
        function(callback) {
            if ( options.cas ) {
                Model.unlock( bucket, id, options.cas, options, function( err) {
                    if ( err ) {
                        callback(err);
                    } else {
                        Model.findById(bucket, id, callback);
                    }
                } )
            } else {
                Model.findById(bucket, id, callback);
            }
        },
        function(data, callback) {
            _data = _.omit(_data, internals.lockedProperties);
            if(options.noDeep){
                _data = _.defaults(_data, data);
            }
            else{
                _data = _.defaultsDeep(_data, data);
            }

            _data._updateDate = Math.round(+new Date()/1000);
            _data._updateUser = options.updateUser || internals.SYSTEM_USER;

            var errors = schema.validate(_data);
            if(!errors) {
                Model.DB.updateWithIndexingViews(bucket, id, _data, function (err, result) {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, result);
                    }
                }, options);
            }else{
                callback(errors, null);
            }
        }
    ], cb);
};

Model.remove = function(id, bucket, cb) {
    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, callback);
        },
        function(data, callback){
            Model.DB.remove(bucket, id, callback, true);
        }
    ], function(err, results){
        cb(err, results);
    });
};

Model.removeWithIndexingViews = function(id, bucket, cb, options) {
    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, callback);
        },
        function(data, callback){
            Model.DB.removeWithIndexingViews(bucket, id, callback, options);
        }
    ], function(err, results){
        cb(err, results);
    });


};

Model.counter = function(bucket, id, delta, cb){
    Model.DB.counter(bucket, id, delta, cb);
};

Model.unlock = function( bucket, id, cas, options, cb ) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    }
    options = options || {};

    Model.DB.unlock( bucket, id, cas, options, cb );
};

Model.lock = function( bucket, id, options, cb ) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    }
    options = options || {};

    Model.DB.lock( bucket, id, options, cb );
};

exports = module.exports = Model;
