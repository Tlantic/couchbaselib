var _ = require('lodash');
var util = require('util');
var async = require('async');
var uuid = require('uuid');

var internals = {

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

Model.findById = function(bucket, key, cb) {
    Model.DB.get(bucket, key, function(err, result) {
        if (err) {
            cb(err);
        } else {
            cb(null, result);
        }
    });
};


Model.prototype.save = function(cb) {
    this.data._uId = uuid.v4();
    this.data._createDate = Math.round(+new Date()/1000);
    this.data._updateDate = Math.round(+new Date()/1000);

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.insert(this.bucket, this.createKey(this.data._uId), this.data, function(err, result) {
            if (err) {
                cb(err);
            } else {
                cb(null, result);
            }
        }, true);
    } else {
        cb(errors, null);
    }

};

Model.update = function(id, data, bucket, schema, cb) {
    var _data = data;
    var errors = schema.validate(data);
    if(!errors){
        async.waterfall([
            function(callback) {
                Model.findById(bucket, id, callback);
            },
            function(data, callback) {
                _data = _.omit(_data, ['_uId', '_type', '_createDate']);
                _data = _.pick(_.defaults(_data, data), _.keys(data));
                _data._updateDate = Math.round(+new Date()/1000);

                Model.DB.update(bucket, id, _data, function(err, result) {
                    if (err) {
                        cb(err);
                    } else {
                        cb(null, result);
                    }
                }, true);
            }
        ], function(err, results) {
            cb(err, results);
        });
    }else{
        cb(errors, null);
    }

};

Model.remove = function(id, bucket, cb) {
    Model.DB.remove(bucket, id, function(err, result) {
        if (err) {
            cb(err);
        } else {
            cb(null, result);
        }
    }, true);
};

exports = module.exports = Model;
