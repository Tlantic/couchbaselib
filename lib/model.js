var _ = require('lodash');
var util = require('util');
var async = require('async');


var internals = {

};

function Model(data, connection) {
    this.data = this.sanitize(data);
    return;
}


Model.prototype.sanitize = function(data) {
    data = data || {};
    return data;
};


internals.createKey = function(id) {
    return Model.key + '::' + id;
};

/*
 * Models
 */

Model.findById = function(key, cb) {
    Model.DB.get(Model.bucket, internals.createKey(key), function(err, result) {
        if (err) {
            cb(err);
        } else {
            cb(null, result);
        }
    });
};


Model.prototype.save = function(cb) {
    var errors = Model.schema.validate(this.data);
    if (!errors) {
        Model.DB.insert(Model.bucket, internals.createKey(this.data._uId), this.data, function(err, result) {
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

Model.update = function(id, data, cb) {
    var _data = data;
    async.waterfall([
        function(callback) {
            Model.findById(id, callback);
        },
        function(data, callback) {
            _data = _.pick(_.defaults(_data, data), _.keys(data));

            Model.DB.update(Model.bucket, id, _data, function(err, result) {
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
};

Model.remove = function(id, cb) {
    Model.DB.remove(Model.bucket, internals.createKey(id), function(err, result) {
        if (err) {
            cb(err);
        } else {
            cb(null, result);
        }
    }, true);
};

exports = module.exports = Model;
