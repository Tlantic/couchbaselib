var uuid = require('uuid');    
var jjv = require('jjv');



var internals = {
    schema: {},
    key:'_model_',
    env:{}
};


function Schema(key, info) {
    internals.env = jjv();
    info.properties.uId = {
        type: 'string',
        default: uuid.v4()
    };

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



module.exports = Schema;


exports = module.exports = Schema;
