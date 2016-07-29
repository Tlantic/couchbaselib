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

/**
 * Retrieves a document by its document id
 * @param bucket
 * @param key
 * @param options
 * @param cb
 */
Model.findById = function(bucket, key, options, cb) {

    if (cb === void 0) {
        cb = options;
        options = {};
    }

    Model.DB.get(bucket, key, options, cb);
};

/**
 * Retrieves and locks a document by its document id
 * @param bucket
 * @param key
 * @param options
 * @param cb
 */
Model.findByIdAndLock = function(bucket, key, options, cb) {
    Model.DB.getAndLock(bucket, key, options, cb);
};

/**
 * Retrieves multiple documents matching the specified key
 * @param bucket
 * @param key
 * @param options
 * @param cb
 */
Model.getMulti = function(bucket, key, options, cb) {
    Model.DB.getMulti(bucket, key, options, cb);
};


/**
 * Saves data into the db
 * @param options
 * @param cb
 */
Model.prototype.save = function(options, cb ) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    options.retrieve =  options.retrieve !== void 0 ? options.retrieve : true;


    this.data._uId = uuid.v4();
    this.data._createDate = this.data._updateDate =  Math.round(+new Date()/1000);
    this.data._createUser = this.data._updateUser =  options.updateUser || internals.SYSTEM_USER;

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.insert(this.bucket, this.createKey(this.data._uId), this.data, options, cb, options);
    } else {
        cb(errors, null);
    }

};

/**
 * Saves data into the db and indexes
 * @param options
 * @param cb
 */
Model.prototype.saveWithIndexingViews = function(views, options, cb) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    options.retrieve =  options.retrieve !== void 0 ? options.retrieve : true;

    this.data._uId = uuid.v4();
    this.data._createDate = this.data._updateDate =  Math.round(+new Date()/1000);
    this.data._createUser = this.data._updateUser =  options.updateUser || internals.SYSTEM_USER;

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.insertWithIndexingViews(this.bucket, this.createKey(this.data._uId), this.data, views, options, cb, options);
    } else {
        cb(errors, null);
    }

};

/**
 * Updates a document
 * @param id
 * @param data
 * @param bucket
 * @param schema
 * @param cb
 * @param options
 */
Model.update = function(id, data, bucket, schema, options, cb) {

    var _data = data;

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    options.retrieve =  options.retrieve !== void 0 ? options.retrieve : true;

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
                Model.DB.update(bucket, id, _data, options, cb);
            }else{
                cb(errors, null);
            }
        }
    ], cb);

};

/**
 * Updates a document and indexes the specified views
 * @param id
 * @param data
 * @param bucket
 * @param schema
 * @param views
 * @param options
 * @param cb
 */
Model.updateWithIndexingViews = function(id, data, bucket, schema, views, options, cb) {

    var _data = data;

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    options.retrieve =  options.retrieve !== void 0 ? options.retrieve : true;

    async.waterfall([
        function(callback) {
            if ( options.cas ) {
                Model.unlock( bucket, id, options.cas, options, function( err) {
                    if ( err ) {
                        callback(err);
                    } else {
                        Model.findById(bucket, id, options, callback);
                    }
                } )
            } else {
                Model.findById(bucket, id, options, callback);
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
                Model.DB.updateWithIndexingViews(bucket, id, _data, views, options, callback);
            }else{
                callback(errors, null);
            }
        }
    ], cb);
};

/**
 *
 * @param id
 * @param bucket
 * @param options
 * @param cb
 */
Model.remove = function(id, bucket, options, cb) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }


    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, options, callback);
        },
        function(data, callback){
            Model.DB.remove(bucket, id, options, callback);
        }
    ], cb);
};

Model.removeWithIndexingViews = function(id, bucket, views, options, cb) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    options.retrieve =  options.retrieve !== void 0 ? options.retrieve : true;

    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, options, callback);
        },
        function(data, callback){
            Model.DB.removeWithIndexingViews(bucket, id, views, options, callback);
        }
    ], cb);


};

Model.counter = function(bucket, id, delta, cb){
    Model.DB.counter(bucket, id, delta, cb);
};

Model.unlock = function( bucket, id, cas, options, cb ) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    Model.DB.unlock( bucket, id, cas, options, cb );
};

Model.lock = function( bucket, id, options, cb ) {

    if ( cb === void 0 ) {
        cb = options;
        options = {};
    } else {
        options = options || {};
    }

    Model.DB.lock( bucket, id, options, cb );
};

exports = module.exports = Model;
