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

Model.getMulti = function(bucket, key, cb) {
    Model.DB.getMulti(bucket, key, function(err, result) {
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

Model.prototype.saveWithIndexingViews = function(cb, views) {
    this.data._uId = uuid.v4();
    this.data._createDate = Math.round(+new Date()/1000);
    this.data._updateDate = Math.round(+new Date()/1000);

    var errors = this.schema.validate(this.data);
    if (!errors) {
        Model.DB.inserteWithIndexingViews(this.bucket, this.createKey(this.data._uId), this.data, function(err, result) {
            if (err) {
                cb(err);
            } else {
                cb(null, result);
            }
        }, true, views);
    } else {
        cb(errors, null);
    }

};

Model.update = function(id, data, bucket, schema, cb) {
    var _data = data;

    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, callback);
        },
        function(data, callback) {
            _data = _.omit(_data, ['_uId', '_type', '_createDate']);
            _data = _.pick(_.defaults(_data, data), _.keys(data));
            _data._updateDate = Math.round(+new Date()/1000);
            var errors = schema.validate(_data);
            if(!errors) {
                Model.DB.update(bucket, id, _data, function (err, result) {
                    if (err) {
                        cb(err);
                    } else {
                        cb(null, result);
                    }
                }, true);
            }else{
                cb(errors, null);
            }
        }
    ], function(err, results) {
        cb(err, results);
    });

};

Model.updateWithIndexingViews = function(id, data, bucket, schema, cb,views) {
    var _data = data;

    async.waterfall([
        function(callback) {
            Model.findById(bucket, id, callback);
        },
        function(data, callback) {
            _data = _.omit(_data, ['_uId', '_type', '_createDate']);
            _data = _.pick(_.defaults(_data, data), _.keys(data));
            _data._updateDate = Math.round(+new Date()/1000);
            var errors = schema.validate(_data);
            if(!errors) {
                Model.DB.updateWithIndexingViews(bucket, id, _data, function (err, result) {
                    if (err) {
                        cb(err);
                    } else {
                        cb(null, result);
                    }
                }, true, views);
            }else{
                cb(errors, null);
            }
        }
    ], function(err, results) {
        cb(err, results);
    });

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

Model.removeWithIndexingViews = function(id, bucket, cb, views) {
    Model.DB.removeWithIndexingViews(bucket, id, function(err, result) {
        if (err) {
            cb(err);
        } else {
            cb(null, result);
        }
    }, views);
};

exports = module.exports = Model;
