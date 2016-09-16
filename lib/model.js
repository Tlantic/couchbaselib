/**
 * @module Model
 */

'use strict';
const
	_ = require( 'lodash' ),
	uuid = require( 'uuid' ),
	fnForEach = Array.prototype.forEach,

	$KEY = Symbol(),
	$CAS = Symbol(),
	_contexts = new WeakMap();


/**
 * CouchBase document type, kept at the _type
 * property of any model.
 *
 * @typedef {Function} Bucket.OpCallback
 *
 * @see http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#OpCallback
 * @external Bucket.OpCallback
 * @since 2.0.0
 */



/**
 * CouchBase document type, kept at the _type
 * property of any model.
 *
 * @typedef {string} DocumentType
 *
 * @since 2.0.0
 */

/**
 * CouchBase document unique id, kept at the _uId
 * property of any model.
 *
 * @typedef {string} DocumentUUID
 *
 * @since 2.0.0
 */



/**
 * Model context
 *
 * @typedef {object} ModelContext
 * @param {CouchBaseDB} ModelContext.db - Cluster connection
 * @param {string} ModelContext.bucket - Bucket name
 * @param {Object} ModelContext.fields - Map of property names to lookup for metadata (i.e. uuid, createdOn)
 *  @param {string} [ModelContext.fields.uuid="_uId"] - Property name for the [uuid]{@see DocumentUUID}
 *  @param {string} [ModelContext.fields.uuid="_type"] - Property name for the document [type]{@see DocumentType}
 *  @param {string} [ModelContext.fields.createdOn="_createDate"] - Property name for createdOn timestamp
 *  @param {string} [ModelContext.fields.updateOn="_updateDate"] - Property name for updatedOn timestamp
 *  @param {string} [ModelContext.fields.createdBy="_createUser"] - Property name for createdBy reference
 *  @param {string} [ModelContext.fields.createdBy="_updateUser"] - Property name for updatedBy reference
 * @since 2.0.0
 */



/**
 * Creates an interceptor for
 * [OpCallback]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#OpCallback}.
 * Upon a successful response, the value of the document is merged
 * with the [instance]{@see Model}.
 *
 * @param {Model} instance
 * @param {Object} options
 * @param {CouchBaseDB.OpCallback|Bucket.OpCallback|Function} callback
 * @returns {Model.OpCallback}
 *
 * @private
 * @since 2.0.0
 */
function createOpCallbackInterceptor( instance, options, callback ) {

	if ( arguments.length < 3 ) {
		//noinspection JSValidateTypes
		callback = options;
		options = {}
	} else if ( !options ) {
		options = {};
	}

	/**
	 * Single-Key callback.
	 *
	 * @typedef {Function} Model.OpCallback
	 * @param {undefined|Error} error
	 *  The error for the operation. This can either be an Error object
	 *  or a value which evaluates to false (null, undefined, 0 or false).
	 * @param {Object} result
	 *  The result of the operation that was executed.  This usually contains
	 *  at least a <i>cas</i> property, and on some operations will contain a
	 *  <i>value</i> and <i>hit</hit> ([document id]{@see DocumentKey}) property.
	 *
	 *  @since 2.0.0
	 */
	return function opCallback( err, res ) {
		let value;

		if ( err ) {
			callback( err );
		} else {

			value = res.value;
			for ( const propName in value ) {
				if ( value.hasOwnProperty( propName ) ) {
					instance[ propName ] = value[ propName ];
				}
			}
			instance[ $CAS ] = res.cas;
			res.value = instance;

			callback( void 0, options.returnOnlyValues ? res.value : res );
		}
	};
}


/**
 * Creates an interceptor for
 * [MultiGetCallbacks]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#MultiGetCallback}.
 * Upon a successful response, the value of the documents is merged with the corresponding [instances]{@see Model}.
 *
 * @param {Model[]} instances
 * @param {Object} options
 * @param {Bucket.MultiGetCallback|CouchBaseDB.MultiGetCallback|Function} callback
 * @returns {Model.MultiGetCallback}
 *
 * @private
 * @since 2.0.0
 */
function createMultiGetCallbackInterceptor( instances, options, callback ) {

	let
		TypeModel = instances[0].constructor,
		context = TypeModel.getContext(),
		uuidPropertyName = context.fields.uuid;

	if ( arguments.length < 3 ) {
		//noinspection JSValidateTypes
		callback = options;
		options = {}
	} else if ( !options ) {
		options = {};
	}

	/**
	 * Multi-Key callback.
	 *
	 * @typedef {Function} Model.MultiGetCallback
	 * @param {undefined|Error} error
	 *  The error for the operation. This can either be an Error object
	 *  or a value which evaluates to false (null, undefined, 0 or false).
	 * @param {Object} result
	 *  The result of the operation that was executed.  This usually contains
	 *  at least a <i>cas</i> property, and on some operations will contain a
	 *  <i>value</i> and <i>hit</hit> ([document id]{@see DocumentKey}) property.
	 *
	 *  @since 2.0.0
	 */
	return function multiGetCallback( err, res ) {

		if ( err ) {
			callback( err );
		} else if (res.length === 0) {

			callback(void 0, res);
		} else {



			res.forEach( ( data ) => {

				const
					cas = data.cas,
					value = data.value;

				const
					targets = instances.filter( c => c[uuidPropertyName] === value[uuidPropertyName] );

				targets.forEach( instance => {

					instance[ $CAS ] = cas;

					for ( const propName in value ) {
						if ( value.hasOwnProperty( propName ) ) {
							instance[ propName ] = value[ propName ]
						}
					}
				} );

			}, instances );


			callback( void 0, options.returnOnlyValues ?
				instances : instances.map( instance => {
				return {
					cas: instance.getCAS(),
					hit: instance[uuidPropertyName],
					value: instance
				}
			} ) );
		}
	};
}


/**
 * Creates an interceptor for
 * [QueryCallback]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#QueryCallback}.
 * Upon a successful response, each hit is instantiated as [model]{@see Model}.
 *
 * @param {Function} TypeModel Class
 * @param {Object} options
 * @param {Bucket.QueryOpCallback|CouchBaseDB.QueryOpCallback|Function} callback
 * @returns {Model.QueryOpCallback}
 *
 * @private
 * @since 2.0.0
 */
function createQueryCallbackInterceptor( TypeModel, options, callback ) {

	if ( arguments.length < 3 ) {
		//noinspection JSValidateTypes
		callback = options;
		options = {}
	} else if ( !options ) {
		options = {};
	}

	/**
	 * Single-Key callback.
	 *
	 * @typedef {Function} Model.QueryOpCallback
	 * @param {undefined|Error} error
	 *  The error for the operation. This can either be an Error object
	 *  or a value which evaluates to false (null, undefined, 0 or false).
	 * @param {Object} result
	 *  The result of the operation that was executed.  This usually contains
	 *  at least a <i>cas</i> property, and on some operations will contain a
	 *  <i>value</i> and <i>hit</hit> document([id]{@see DocumentKey}) property.
	 *
	 *  @since 2.0.0
	 */
	return function queryCallback( err, res ) {

		if ( err ) {
			callback( err );
		} else if ( options.returnOnlyValues ) {

			callback(void 0, res.map( r => (r.value && new TypeModel(r.value)) || r ));
		} else {

			res.forEach( r => r.value ? r.value = new TypeModel(r.value) : void 0 );
			callback( void 0, res );
		}

	};
}



class Model {

// ---------------------------------------------- BEGIN Class Properties -------------------------------------------- //

// ----------------------------------------------- END Class Properties --------------------------------------------- //



// ---------------------------------------------- BEGIN Class Methods ----------------------------------------------- //

	/**
	 * Sets the execution context for this class
	 *
	 * @param {CouchBaseDB} db
	 * @param {string} bucket
	 * @param {object} fields
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static setContext( db, bucket, fields) {


		if ( !fields ) {
			fields  = {
				uuid: '_uId',
				type: '_type',
				createdOn: '_createDate',
				updatedOn: '_updateDate',
				createdBy: '_createUser',
				updatedBy: '_updateUser'
			};
		} else {
			fields.uuid = fields.uuid || '_uId';
			fields.type = fields.type || '_type';
			fields.createdOn = fields.createdOn || '_createDate';
			fields.updatedOn = fields.updatedOn || '_updateDate';
			fields.createdBy = fields.createdBy || '_createUser';
			fields.updatedBy = fields.updatedBy || '_updateUser';
		}

		_contexts.set( this, { db: db, bucket: bucket, fields: fields });

		return this;
	}


	/**
	 * Retrieves the execution context of this class
	 *
	 * @returns {ModelContext}
	 *
	 * @since 2.0.0
	 */
	static getContext() {
		const
			context = _contexts.get( this );

		if ( context === void 0 ) {
			throw new Error( `Context for "${this.name}" is yet to be defined.` );
		}
		return context;
	}


	/**
	 * Constructs a unique key for the document.
	 *
	 * @param {DocumentUUID} uuid - A unique id within the given type.
	 * @returns {DocumentKey} - The id of the document.
	 *
	 * @virtual
	 * @since 2.0.0
	 */
	static modelKey( uuid ) {
		return uuid;
	}


	/**
	 * * Sanitizes an object content
	 *
	 * @param {Model} obj
	 * @param {Function} callback
	 * @return {Model}
	 *
	 * @virtual
	 * @since 2.0.0
	 */
	static sanitize( obj, callback ) {
		callback(void 0, obj);
		return this;
	}


	/**
	 * Validates object contents against a schema.
	 *
	 * @param obj
	 * @param {Function} callback
	 * @return {Model}
	 *
	 * @virtual
	 * @since 2.0.0
	 */
	static validate( obj, callback ) {
		callback(void 0, obj);
		return this;
	}


	/**
	 * Performs a insert operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {DocumentUUID} uId - Document unique id.
	 * @param {Object} value - Document content.
	 * @param {Object} [options]
	 * @param {Bucket.OpCallback} callback
	 * @return {Model}
	 *
	 * @since 2.0.0
	 */
	static insert( uId, value, options, callback ) {

		const
			TypeModel = this;

		if ( arguments.length < 4 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		return new TypeModel( uId, value ).save( options, callback );
	}


	/**
	 * Performs a get operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {DocumentUUID} uId - Document unique id.
	 * @param {Object} [options]
	 * @param {Bucket.OpCallback|Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static get( uId, options, callback ) {

		const
			TypeModel = this;

		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		return new TypeModel( uId ).update( options, callback );
	}


	/**
	 * Performs a get operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {DocumentUUID|DocumentUUID[]} uIds - Documents unique id.
	 * @param {Object} [options={}]
	 * @param {Bucket.MultiGetCallback|Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static getMulti( uIds, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();

		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		if ( uIds.constructor !== Array ) {
			uIds = [ uIds ];
		}


		let
			instances = uIds.map( ( uId ) => new TypeModel( uId ) );


		//noinspection JSCheckFunctionSignatures
		context.db.getMulti( context.bucket, uIds.map( TypeModel.modelKey ), options,
			createMultiGetCallbackInterceptor( instances, {returnOnlyValues: options.returnOnlyValues}, callback ) );

		return this;
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Performs a replace operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {DocumentUUID} uId - Document unique id.
	 * @param {Object} value - Document content.
	 * @param {Object} [options]
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static replace( uId, value, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();


		if ( arguments.length < 4 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		//noinspection JSCheckFunctionSignatures
		context.db.replace( context.bucket, TypeModel.modelKey( uId ), value, options, ( err, res ) => {
			if ( err ) {
				return callback( err );
			}

			if ( res && res.value ) {
				res.value = new TypeModel( res.value );
			}

			callback( void 0, res );
		} );

		return this;
	}


	/**
	 * Performs a remove operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {DocumentUUID} uId - Document unique id.
	 * @param {Object} [options={}]
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static remove( uId, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();


		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		context.db.remove( context.bucket, TypeModel.modelKey( uId ), options, ( err, res ) => {
			if ( err ) {
				return callback( err );
			}

			if ( res && res.value ) {
				res.value = new TypeModel( res.value );
			}

			callback( void 0, res );
		} );

		return this;
	}


	/**
	 * Performs a query operation directly
	 * in the specified bucket.
	 *
	 * NOTE: The returning value is intercepted
	 * in order for to transform the value property
	 * content into a new instance of this type.
	 *
	 * @param {Object} query.
	 * @param {Object} [options={}]
	 * @param {Bucket.MultiGetCallback|Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	static query( query, options, callback ) {


		const
			TypeModel = this,
			context = TypeModel.getContext();

		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		context.db.query( context.bucket, query, options,
			createQueryCallbackInterceptor( TypeModel, { returnOnlyValues: options.returnOnlyValues }, callback ) );

		return this;
	}

// ----------------------------------------------- END Class Methods ------------------------------------------------ //


// ------------------------------------------------ BEGIN Constructor ----------------------------------------------- //
	/**
	 * Constructs the model defining its context and schema.
	 *
	 * @param {string} name - Model name.
	 * @param {string} [uId=uuid.v4()] - Instance uniqueId
	 * @constructs Model
	 * @since 2.0.0
	 */
	constructor( name, uId ) {

		const
			fields = this.constructor.getContext().fields;


		/**
		 * Document unique ID
		 * @type {DocumentUUID}
		 *
		 * @readonly
		 * @since 2.0.0
		 */
		delete this[fields.uuid];
		//noinspection JSUnresolvedFunction
		this[fields.uuid] = uId || uuid.v4();

		/**
		 * Document type extracted from schema.
		 * @type {string}
		 *
		 * @readonly
		 * @since 2.0.0
		 */
		delete this[fields.type];
		this[fields.type] = name;

	}

// ------------------------------------------------- END Constructor ------------------------------------------------ //


// -------------------------------------------- BEGIN Instance Properties ------------------------------------------- //
	/**
	 * Returns the document key
	 *
	 * @returns {DocumentKey}
	 *
	 * @private
	 * @since 2.0.0
	 */
	get [$KEY]() {
		const
			TypeModel = this.constructor;

		return TypeModel.modelKey( this[TypeModel.getContext().fields.uuid] );
	}


// --------------------------------------------- END Instance Properties -------------------------------------------- //


// --------------------------------------------- BEGIN Instance Methods --------------------------------------------- //

	/**
	 * Returns a cached CAS from last execution.
	 *
	 * @returns {Bucket.CAS}
	 *
	 * @since 2.0.0
	 */
	getCAS() {
		return this[ $CAS ];
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Sanitizes data
	 *
	 * @param {Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	sanitize(callback) {
		this.constructor.sanitize( this, callback );
		return this;
	}


	/**
	 * Validates data
	 *
	 * @param {Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	validate(callback) {
		this.constructor.validate( this, callback);
		return this
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Merge src object properties with the current instance
	 * properties ignoring the ones where name starts with _.
	 *
	 * @param {...Object} src
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	merge( src /*, src2, ..., srcN */ ) {
		fnForEach.call( arguments, ( src ) => {
			for ( const propName in src ) {
				if ( propName[ 0 ] !== '_' && src.hasOwnProperty( propName ) ) {
					//noinspection JSUnresolvedFunction
					this[ propName ] = _.cloneDeep( src[ propName ] );
				}
			}
		} );
		return this;
	}


	/**
	 * Retrieves a document and updates the instance.
	 *
	 * @param {Object} [options]
	 *  @param {boolean} [options.returnOnlyValues=false]
	 *      Returns only value property of the result.
	 *  @param {Bucket.CAS} [options.cas]
	 *      Unlocks the document if in a locked state.
	 *  @param {boolean} [options.lock=false]
	 *      Locks the document until a write or unlock {@see Model#unlock}
	 *      operation with a valid [CAS]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#toc36}
	 *      takes place.
	 *  @param {number} [options.lockTime=15]
	 *      For how many seconds the document will remain locked.
	 * @param {Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	update( options, callback ) {

		const
			_options = {},
			context = this.constructor.getContext();


		if ( arguments.length < 2 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		if ( options.lock === true ) {

			if ( options.cas !== void 0 ) {
				_options.cas = options.cas;
			}

			if ( options.lockTime ) {
				_options.lockTime = options.lockTime;
			}

			context.db.getAndLock( context.bucket, this[ $KEY ], _options,
				createOpCallbackInterceptor( this, {
					returnOnlyValues: options.returnOnlyValues
				}, callback ) );
		} else {

			context.db.get( context.bucket, this[ $KEY ], _options,
				createOpCallbackInterceptor( this, {
					returnOnlyValues: options.returnOnlyValues
				}, callback ) );
		}


		return this;
	}


	/**
	 * Upserts the current instance state.
	 *
	 * @param {Object} [options]
	 *  @param {boolean} [options.populate=true]
	 *      Retrieve content of newly created document.
	 *  @param {boolean} [options.returnOnlyValues=false]
	 *      Returns only value property of the result.
	 *  @param {Bucket.CAS} [options.cas]
	 *      Unlocks the document if in a locked state.
	 *  @param {number} [options.expiry=0]
	 *      Document TTL.
	 *  @param {number} [options.persist_to=0]
	 *      Ensures this operation is persisted to this many nodes.
	 *  @param {number} [options.replicate_to=0]
	 *      Ensures this operation is replicated to this many nodes.
	 *  @param {string} [options.user=""]
	 *      Value to be given to properties like _updateUser.
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	save( options, callback ) {

		const
			_options = {},
			snapshot = {},
			timestamp = Date.now(),
			context = this.constructor.getContext(),
			propertyNames = context.fields;


		if ( arguments.length < 2 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		if ( options.cas ) {
			_options.cas = options.cas;
		}

		_options.populate = options.populate;

		_options.user = options.user || "";
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;


		snapshot.createdOn = this[propertyNames.createdOn];
		snapshot.createdBy = this[propertyNames.createdBy];
		snapshot.updatedOn = this[propertyNames.updatedOn];
		snapshot.updatedBy = this[propertyNames.updatedBy];


		if ( snapshot.createdOn === void 0 ) {
			this[propertyNames.createdOn] = timestamp;
			this[propertyNames.createdBy] = _options.user;
		}

		this[propertyNames.updatedOn] = timestamp;
		this[propertyNames.updatedBy] = _options.user;


		//noinspection JSUnusedLocalSymbols
		this.validate( ( error, result ) => {
			if ( error ) {
				if ( snapshot.createdOn === void 0 ) {

					delete this[propertyNames.createdOn];
					delete this[propertyNames.createdBy];
					delete this[propertyNames.updatedOn];
					delete this[propertyNames.updatedBy];
				} else {

					this[propertyNames.createdOn] = snapshot.createdOn;
					this[propertyNames.createdBy] = snapshot.createdBy;
					this[propertyNames.updatedOn] = snapshot.updatedOn;
					this[propertyNames.updatedBy] = snapshot.updatedBy;
				}

				return callback( error );
			} else {

				context.db.upsert( context.bucket, this[ $KEY ], this, _options, createOpCallbackInterceptor( this, {
					returnOnlyValues: options.returnOnlyValues
				}, callback ));
			}
		});

		return this;
	}


	/**
	 * Removes this instance.
	 *
	 * @param {Object} [options]
	 *  @param {Bucket.CAS} [options.cas]
	 *      Unlocks the document if in a locked state.
	 *  @param {number} [options.expiry=0]
	 *      Document TTL.
	 *  @param {number} [options.persist_to=0]
	 *      Ensures this operation is persisted to this many nodes.
	 *  @param {number} [options.replicate_to=0]
	 *      Ensures this operation is replicated to this many nodes.
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	remove( options, callback ) {

		const
			_options = {},
			context = this.constructor.getContext();


		if ( arguments.length < 2 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		if ( options.cas ) {
			_options.cas = options.cas;
		}
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;


		context.db.remove( context.bucket, this[ $KEY ], _options, callback );

		return this;
	}


	/**
	 * Retrieves this instance counter value.
	 *
	 * @param {number} increment
	 * @param {Object} [options]
	 *  @param {string} [options.key="counter"]
	 *      Appends to the current document key <document_key>-<key> to hold the counter value.
	 *  @param {initial} [options.initial=undefined]
	 *      Unlocks the document if in a locked state.
	 *  @param {number} [options.expiry=0]
	 *      Document TTL.
	 *  @param {number} [options.persist_to=0]
	 *      Ensures this operation is persisted to this many nodes.
	 *  @param {number} [options.replicate_to=0]
	 *      Ensures this operation is replicated to this many nodes.
	 * @param {Function} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	counter( increment, options, callback ) {

		const
			_options = {},
			context = this.constructor.getContext();


		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		if ( options.initial >= 0 ) {
			_options.initial = options.initial;
		}

		_options.expiry = options.expiry || 0;
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		context.db.counter( context.bucket, `${this[ $KEY ]}-${options.key || 'counter'}`, increment, _options, callback );
		return this;
	}


	/**
	 * Unlocks the document.
	 *
	 * @param {Bucket.CAS} cas
	 * @param {Object} [options]
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	unlock( cas, options, callback ) {

		const
			context = this.constructor.getContext();


		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		context.db.unlock( context.bucket,  this[ $KEY ], cas, options, callback );
		return this;
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Touch the document defining a new TTL or disabling TTL if 0.
	 *
	 * @param {number} expiry - Document TTL
	 * @param {Object} [options]
	 *  @param {number} [options.persist_to=0]
	 *      Ensures this operation is persisted to this many nodes.
	 *  @param {number} [options.replicate_to=0]
	 *      Ensures this operation is replicated to this many nodes.
	 * @param {Bucket.OpCallback} callback
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	touch( expiry, options, callback ) {

		const
			_options = {},
			context = this.constructor.getContext();

		if ( arguments.length < 3 ) {
			//noinspection JSValidateTypes
			callback = options;
			options = {}
		} else if ( !options ) {
			options = {};
		}

		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		context.db.touch( context.bucket, this[ $KEY ], _options, callback );
		return this;
	}

// ---------------------------------------------- END Instance Methods ---------------------------------------------- //
}

module.exports = Model;
