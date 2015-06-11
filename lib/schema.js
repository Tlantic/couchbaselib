var jjv = require('jjv');


function Schema(key, info) {
    this.env = jjv();
    info.properties._uId = {
        type: 'string'
    };
    
    info.properties._type = {
        type: 'string'
    };

    info.properties._createDate = {
        type: 'number'
    };

    info.properties._updateDate = {
        type: 'number'
    };

    this.schema = {
        type: 'object',
        properties: info.properties,
        required: info.required
    };
    this.key = key;
    this.env.addSchema(this.key, this.schema);
    return;
}

Schema.prototype.validate = function (data) {
    return this.env.validate(this.key, data, { checkRequired: true, useDefault: true, removeAdditional: true });
};


module.exports = Schema;


exports = module.exports = Schema;
