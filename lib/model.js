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
 * Model context
 *
 * @typedef {object} ModelContext
 * @param {CouchBaseDB} ModelContext.db - Cluster connection
 * @param {string} ModelContext.bucket - Bucket name
 *
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
 * Creates an interceptor for
 * [OpCallbacks]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#OpCallback}.
 * Upon a successful response, the value of the document is merged
 * with the [context]{@see Model}.
 *
 * @param {Model} context
 * @param {Object} options
 * @param {Bucket.OpCallback|CouchBaseDB.OpCallback|Function} callback
 * @returns {Model.OpCallback}
 *
 * @private
 * @since 2.0.0
 */
function createOpCallbackInterceptor( context, options, callback ) {

	if ( arguments.length < 3 ) {
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
					context[ propName ] = value[ propName ];
				}
			}
			context[ $CAS ] = res.cas;
			res.value = context;

			callback( void 0, options.returnOnlyValues ? res.value : res );
		}
	};
}


/**
 * Creates an interceptor for
 * [MultiGetCallbacks]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#MultiGetCallback}.
 * Upon a successful response, the value of the documents are merged with the [contexts]{@see Model}.
 *
 * @param {Model[]} contexts
 * @param {Object} options
 * @param {Bucket.MultiGetCallback|Function} callback
 * @returns {Model.MultiGetCallback}
 *
 * @private
 * @since 2.0.0
 */
function createMultiGetCallbackInterceptor( contexts, options, callback ) {

	if ( arguments.length < 3 ) {
		callback = options;
		options = {}
	} else if ( !options ) {
		options = {};
	}

	/**
	 * Single-Key callback.
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
		} else {

			res.forEach( ( data ) => {

				const
					cas = data.cas,
					value = data.value;

				const
					targets = contexts.filter( c => c._uId === value._uId );

				targets.forEach( context => {

					context[ $CAS ] = cas;

					for ( const propName in value ) {
						if ( value.hasOwnProperty( propName ) ) {
							context[ propName ] = value[ propName ]
						}
					}
				} );

			}, contexts );


			callback( void 0, options.returnOnlyValues ?
				contexts : contexts.map( c => {
				return {
					cas: c.getCAS(),
					hit: c._uId,
					value: c
				}
			} ) );
		}
	};
}


/**
 * Creates an interceptor for
 * [QueryCallback]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#QueryCallback}.
 * Upon a successful response, the value of the documents are merged with the [contexts]{@see Model}.
 *
 * @param {Function} TypeModel Constructor
 * @param {Object} options
 * @param {Bucket.QueryOpCallback|CouchBaseDB.QueryOpCallback|Function} callback
 * @returns {Model.QueryOpCallback}
 *
 * @private
 * @since 2.0.0
 */
function createQueryCallbackInterceptor( TypeModel, options, callback ) {

	if ( arguments.length < 3 ) {
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
	 *  <i>value</i> and <i>hit</hit> ([document id]{@see DocumentKey}) property.
	 *
	 *  @since 2.0.0
	 */
	return function queryCallback( err, res ) {

		if ( err ) {
			callback( err );
		} else if ( options.returnOnlyValues ) {

			callback(void 0, res.map( r => new TypeModel(r.value) ));
		} else {
			res.forEach( r => r.value = new TypeModel(r.value) );
			callback( void 0, res );
		}

	};
}


/**
 * Class that represents a modeled document.
 *
 * @class Model
 * @abstract
 * @since 2.0.0
 */
class Model {


// ---------------------------------------------- BEGIN Class Methods ----------------------------------------------- //
	/**
	 * Sets the execution context for this class
	 *
	 * @param {CouchBaseDB} db
	 * @param {string} bucket
	 *
	 * @protected
	 * @since 2.0.0
	 */
	static setContext( db, bucket ) {
		_contexts.set( this, {
			db: db,
			bucket: bucket
		} );
	}


	/**
	 * Retrieves the execution context of this class
	 *
	 * @returns {ModelContext}
	 *
	 * @private
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
	 */
	static modelKey( uuid ) {
		return uuid;
	}


	/**
	 * * Sanitizes an object content
	 *
	 * @param {Model} obj
	 * @returns {Model}
	 *
	 * @abstract
	 * @since 2.0.0
	 */
	static sanitize( obj ) {
		return obj;
	}


	/**
	 * Validates object contents against a schema.
	 *
	 * @param obj
	 * @param {Function} callback
	 *
	 * @abstract
	 * @since 2.0.0
	 */
	static validate( obj, callback ) {
		callback(void 0, obj);
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

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}

		return new TypeModel( uId, value )
			.save( options, callback );
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

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
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
	 * @param {DocumentUUID} uIds - Documents unique id.
	 * @param {Object} [options]
	 * @param {Bucket.MultiGetCallback|Function} callback
	 */
	static getMulti( uIds, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();

		if ( arguments.length < 3 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}

		if ( uIds.constructor !== Array ) {
			uIds = [ uIds ];
		}

		let
			instances = uIds.map( ( uId ) => new TypeModel( uId ) );

		context.db.getMulti( context.bucket, uIds.map( TypeModel.modelKey ), options,
			createMultiGetCallbackInterceptor( instances, { returnOnlyValues: options.returnOnlyValues }, callback ) );

	}


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
	 */
	static replace( uId, value, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();


		if ( arguments.length < 4 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}

		context.db.replace( context.bucket, TypeModel.modelKey( uId ), value, options, ( err, res ) => {
			if ( err ) {
				return callback( err );
			}

			if ( res && res.value && res.value._uId ) {
				res.value = new TypeModel( res.value );
			}

			callback( void 0, res );
		} );

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
	 * @param {Object} [options]
	 * @param {Bucket.OpCallback} callback
	 */
	static remove( uId, options, callback ) {

		const
			TypeModel = this,
			context = TypeModel.getContext();


		if ( arguments.length < 3 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}

		context.db.remove( context.bucket, TypeModel.modelKey( uId ), options, ( err, res ) => {
			if ( err ) {
				return callback( err );
			}

			if ( res && res.value && res.value._uId ) {
				res.value = new TypeModel( res.value );
			}

			callback( void 0, res );
		} );

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
	 * @param {Object} [options]
	 * @param {Bucket.MultiGetCallback|Function} callback
	 */
	static query( query, options, callback ) {


		const
			TypeModel = this,
			context = TypeModel.getContext();

		if ( arguments.length < 3 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		context.db.query( context.bucket, query, options,
			createQueryCallbackInterceptor( TypeModel, { returnOnlyValues: options.returnOnlyValues }, callback ) );

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


		/**
		 * Document unique ID
		 * @type {DocumentUUID}
		 *
		 * @readonly
		 * @since 2.0.0
		 */
		this._uId = uId || uuid.v4();

		/**
		 * Document type extracted from schema.
		 * @type {string}
		 *
		 * @readonly
		 * @since 2.0.0
		 */
		this._type = name;
	}

// ------------------------------------------------- END Constructor ------------------------------------------------ //


// -------------------------------------------- BEGIN Instance Properties ------------------------------------------- //
	/**
	 * Returns the document key
	 *
	 * @returns {DocumentKey}
	 */
	get [$KEY]() {
		return this.constructor.modelKey( this._uId );
	}

// --------------------------------------------- END Instance Properties -------------------------------------------- //


// --------------------------------------------- BEGIN Instance Methods --------------------------------------------- //
	/**
	 * Returns a cached CAS from last execution.
	 *
	 * @returns {Bucket.CAS}
	 */
	getCAS() {
		return this[ $CAS ];
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Sanitizes data.
	 *
	 * @since 2.0.0
	 */
	sanitize() {
		return this.constructor.sanitize( this );
	}


	/**
	 * Validates data
	 *
	 * @param {Function} callback
	 * @returns {Model}
	 */
	validate( callback) {
		return this.constructor.validate( this, callback);
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
			settings = {},
			context = this.constructor.getContext();


		if ( arguments.length < 2 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		if ( options.lock === true ) {

			if ( options.cas !== void 0 ) {
				settings.cas = options.cas;
			}

			if ( options.lockTime ) {
				settings.lockTime = options.lockTime;
			}

			context.db.getAndLock( context.bucket, this[ $KEY ], settings,
				createOpCallbackInterceptor( this, {
					returnOnlyValues: options.returnOnlyValues
				}, callback ) );
		} else {

			context.db.get( context.bucket, this[ $KEY ], settings,
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
			settings = {},
			snapshot = {},
			timestamp = Math.round( +new Date() / 1000 ),
			context = this.constructor.getContext();


		if ( arguments.length < 2 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		if ( options.cas ) {
			settings.cas = options.cas;
		}

		settings.populate = options.populate;

		settings.user = options.user || "";
		settings.persist_to = options.persist_to || 0;
		settings.replicate_to = options.replicate_to || 0;


		snapshot._createDate = this._createDate;
		snapshot._createUser = this._createUser;
		snapshot._updateDate = this._updateDate;
		snapshot._updateUser = this._updateUser;


		if ( this._createDate === void 0 ) {
			this._createDate = timestamp;
			this._createUser = settings.user;
		}

		this._updateDate = timestamp;
		this._updateUser = settings.user;


		//noinspection JSUnusedLocalSymbols
		this.validate( ( error, result ) => {
			if ( error ) {
				if ( snapshot._createDate === void 0 ) {

					delete this._createDate;
					delete this._createUser;
					delete this._updateDate;
					delete this._updateUser;
				} else {

					this._createDate = snapshot._createDate;
					this._createUser = snapshot._createUser;
					this._updateDate = snapshot._updateDate;
					this._updateUser = snapshot._updateUser;
				}

				return callback( error );
			} else {

				context.db.upsert( context.bucket, this[ $KEY ], this, settings, createOpCallbackInterceptor( this, {
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
			settings = {},
			context = this.constructor.getContext();


		if ( arguments.length < 2 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		if ( options.cas ) {
			settings.cas = options.cas;
		}
		settings.persist_to = options.persist_to || 0;
		settings.replicate_to = options.replicate_to || 0;


		context.db.remove( context.bucket, this[ $KEY ], settings, callback );

		return this;
	}


	/**
	 * Retrieves this instance counter value.
	 *
	 * @param {number} increment
	 * @param {Object} [options]
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
			settings = {},
			context = this.constructor.getContext();


		if ( arguments.length < 3 ) {

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}
		

		settings.initial = options.initial;
		settings.expiry = options.expiry || 0;
		settings.persist_to = options.persist_to || 0;
		settings.replicate_to = options.replicate_to || 0;

		context.db.counter( context.bucket, this[ $KEY ], increment, settings, callback );
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

			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		context.db.unlock( context.bucket, Model.modelKey( this._type, this._uId ), cas, options, callback );
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
			settings = {},
			context = this.constructor.getContext();

		if ( arguments.length < 3 ) {
			callback = options;
			options = {}
		} else if ( !options ) {
			options = {};
		}

		settings.persist_to = options.persist_to || 0;
		settings.replicate_to = options.replicate_to || 0;

		context.db.touch( context.bucket, this[ $KEY ], settings, callback );
		return this;
	}

// ---------------------------------------------- END Instance Methods ---------------------------------------------- //
}

module.exports = Model;
