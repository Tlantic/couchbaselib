var jjv = require('jjv');

var internals = {
    schema: {},
    key:'_model_',
    env:{}
};

function Schema(key, info) {
    internals.env = jjv();
    info.properties._uId = {
        type: 'string'
    };
    
    info.properties._type = {
        type: 'string',
        default: key
    };

    info.properties._createDate = {
        type: 'number'
    };

    info.properties._updateDate = {
        type: 'number'
    }

    internals.schema = {
        type: 'object',
        properties: info.properties,
        required: info.required
    };
    this.key = key;
    internals.env.addSchema(internals.key, internals.schema);
    return;
}

Schema.prototype.validate = function (data) {
    return internals.env.validate(internals.key, data, { checkRequired: true, useDefault: true, removeAdditional: true });
};

exports = module.exports = Schema;
