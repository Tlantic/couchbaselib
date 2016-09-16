'use strict';

const
	_ = require( 'lodash' ),
	async = require( 'async' ),
	couchbase = require( 'couchbase' ),


	ViewQuery = couchbase.ViewQuery,
	N1qlQuery = couchbase.N1qlQuery,

	$cluster = Symbol(),
	_buckets = new WeakMap(),


	/**
	 * Default cluster options.
	 *
	 * @type {{host: string, adminPort: number, apiPort: number, sslAdminPort: number, sslApiPort: number, password: null, n1qlPort: number}}
	 *
	 * @private
	 * @since 2.0.0
	 */
	_defaults = {
		host: "localhost",
		adminPort: 8091,
		apiPort: 8092,
		sslAdminPort: 18091,
		sslApiPort: 18092,
		password: null,
		n1qlPort: 8093
	};


/**
 * CouchBase Bucket
 *
 * @external Bucket
 * @see http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html
 *
 * @since 2.0.0
 */

/**
 * CouchBase CAS
 *
 * @external Bucket.CAS
 * @see chbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#toc36
 *
 * @since 2.0.0
 */

/**
 * CouchBase Cluster
 *
 * @external Cluster
 * @see http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Cluster.html
 *
 * @since 2.0.0
 */



/**
 * CouchBase document id. It is a composition of the
 * [model type]{@link DocumentType} concatenated with
 * :: and the document [unique id]{@link DocumentUUID}
 *
 * @typedef {string} DocumentKey
 *
 * @since 2.0.0
 */


/**
 * Adapts the response of the [MultiGetCallback]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#MultiGetCallback}
 * to match the expected return value of a MultiCallback {@see CouchBaseDB.MultiGetCallback}
 * @param {Bucket.MultiGetCallback} callback
 * @returns {CouchBaseDB.MultiGetCallback}
 *
 * @private
 * @since 2.0.0
 */
function createMultiGetCallbackInterceptor( callback ) {

	/**
	 * Multi-Result Callback.
	 *
	 * This callback is used to return results from a getMulti operation.
	 *
	 * @typedef {Bucket.MultiGetCallback} CouchBaseDB.MultiGetCallback
	 *
	 * @param {undefined|Array.<undefined|Error>} errors
	 *  Represents a map of
	 *  errors that occurred during the execution of the operation, if no errors
	 *  did occur its value will be undefined.
	 *  When at least an error occurred, items may vary from undefined (if no
	 *  error occurred for that specified operation) to an actual error at the
	 *  specific request position.
	 * @param {Array.<Object>} results
	 *  Represents a map of results, if an error
	 *  occurred during the execution an item will contain an error property
	 *  containing the error. A successful request will contain a hit, cas and
	 *  value property representing the document id, cas and the actual document
	 *  respectively.
	 *
	 * @since 2.0.0
	 */
	return function multiGetCallback( error, result ) {
		if ( error ) {

			error = [];


			Object.keys( result ).forEach( ( key, idx ) => {

				const
					obj = result[ key ];

				if ( obj.error ) {
					error[ idx ] = obj.error;
				}

				obj.hit = key;

			} );

			callback( error, result );
		} else {

			callback( void 0, Object.keys( result ).map( key => {

				const
					obj = result[ key ];

				obj.hit = key;
				return obj;
			} ) );
		}
	};
}


/**
 * Creates an interceptor for [OpCallbacks]{@link http://docs.couchbase.com/sdk-api/couchbase-node-client-2.0.0/Bucket.html#OpCallback}.
 * Upon a successful response, the value of the document is merged
 * with the [context]{@see Model}.
 *
 * @param {Function} callback
 * @returns {CouchBaseDB.OpCallback}
 *
 * @private
 * @since 2.0.0
 */
function createOpCallbackInterceptor( callback ) {

	/**
	 * Single-Key callback.
	 *
	 * @typedef {Function} CouchBaseDB.OpCallback
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
	return function opCallback( error, result ) {

		if ( error ) {
			callback( error );
		} else if ( result.value ) {
			result.hit = result.value._uId;
			callback( void 0, result );
		} else {
			callback( void 0, result );
		}
	};
}


/**
 * Utility class for perfoming couchbase operations.
 * Connection to the [cluster]{@see Cluster} is performed
 * in a lazy manner by storing the required connection
 * settings.
 *
 *
 * NOTE: Responses from couchbase are intercepted by this
 * class for a more developer friendly format.
 *
 * @class CouchBaseDB

 * @since 2.0.0
 */
class CouchBaseDB {


	/**
	 * [ViewQuery]{@link ViewQuery} class reference.
	 *
	 * @returns {ViewQuery}
	 * @constructor
	 *
	 * @since 2.0.0
	 */
	static get ViewQuery() {
		return ViewQuery;
	}

	//noinspection JSUnusedGlobalSymbols
	/**
	 * [N1qlQuery]{@link ViewQuery} class reference.
	 *
	 * @returns {ViewQuery}
	 * @constructor
	 *
	 * @since 2.0.0
	 */
	static get N1qlQuery() {
		return N1qlQuery
	}


	/**
	 * Provides an established CouchBase cluster connection.
	 * If one doesn't exist, one shall be created using the current
	 * instance properties.
	 *
	 * @returns {Cluster}
	 *
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       cluster = db.connection;
	 *
	 *  let manager;
	 *
	 *  // Get a cluster manager
	 *  manager = cluster.manager();
	 *
	 * @since 2.0.0
	 */
	get connection() {
		if ( this[ $cluster ] === void 0 ) {
			this.initConnection();
		}
		return this[ $cluster ];
	}


	/**
	 * CouchBaseDB constructor
	 *
	 * @param options
	 * @constructs model
	 *
	 * @since 2.0.0
	 */
	constructor( options ) {
		//noinspection JSUnresolvedFunction
		_.defaults( this, options || {}, _defaults );
	}


	/**
	 * Initializes a new couchbase cluster connection.
	 * The connection is initialized using the instance
	 * properties as settings (i.e. host and password).
	 *
	 * NOTE: As an optional way of redefining instance
	 * settings before establishing a connection, you
	 * may passe them as function parameters.
	 *
	 * @param {string} [host="localhost"] - The connection string for your cluster.
	 * @param {string} [password] - The password of your cluster.
	 * @param {number|string} [adminPort=8091] - The cluster administration port
	 * @param {number|string} [apiPort=8092] - The cluster API port
	 * @param {number|string} [sslAdminPort=18091] - The cluster administration port for SSL connections
	 * @param {number|string} [sslApiPort=18092] - The cluster API port for SSL connections
	 * @returns {Cluster}
	 *
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB(),
	 *       cluster = db.initConnection('localhost', 'ruler');
	 *
	 *  let manager;
	 *
	 *  // Get a cluster manager
	 *  manager = cluster.manager();
	 *
	 * @since 2.0.0
	 */
	initConnection( host, password, adminPort, apiPort, sslAdminPort, sslApiPort ) {

		const
			options = {};

		if ( host !== void 0 && host !== null ) {
			this.host = host.trim();
		}

		if ( password !== void 0 && password !== null ) {
			this.password = password;
		}

		if ( apiPort !== void 0 && apiPort !== null ) {
			//noinspection JSUnusedGlobalSymbols
			this.apiPort = apiPort.toString().trim();
		}

		if ( sslAdminPort !== void 0 && sslAdminPort !== null ) {
			//noinspection JSUnusedGlobalSymbols
			this.sslAdminPort = sslAdminPort.toString().trim();
		}

		if ( sslApiPort !== void 0 && sslApiPort !== null ) {
			//noinspection JSUnusedGlobalSymbols
			this.sslApiPort = sslApiPort.toString().trim();
		}


		if ( this.certpath && this.certpath !== null ) {
			options.certpath = this.certpath
		}

		if ( adminPort !== void 0 && adminPort !== null ) {
			this.adminPort = adminPort.toString().trim();
			//noinspection JSUnresolvedFunction
			this[ $cluster ] = new couchbase.Cluster( `${this.host}:${this.adminPort}`, options );
		} else {
			//noinspection JSUnresolvedFunction
			this[ $cluster ] = new couchbase.Cluster( this.host, options );
		}

		//noinspection JSUnresolvedVariable
		_buckets.set( this, this[ $cluster ].connectedBuckets );

		return this[ $cluster ];
	}


	//noinspection JSUnusedGlobalSymbols
	/**
	 * Disconnects all bucket references that were previously set
	 * and associated with the cluster connection.
	 *
	 * @returns {CouchBaseDB}
	 *
	 * @since 2.0.0
	 */
	release() {

		const
			_thisBuckets = _buckets.get( this );

		for ( const bucketName in _thisBuckets ) {
			if ( _thisBuckets.hasOwnProperty( bucketName ) ) {
				_thisBuckets[ bucketName ].disconnect();
				delete _thisBuckets[ bucketName ];
			}
		}

		return this;
	}


	/**
	 * Retrieves a connection to a couchbase bucket.
	 * When called a reference for the bucket is cached for
	 * later retrieval.
	 *
	 * NOTE: If no password is provided or its value is null, the
	 * CouchBaseDB {@link CouchBaseDB} password property will be used
	 * to establish the connection.
	 *
	 * @param {string} name - The name of the bucket to be open/retrieved
	 * @param {string} [password=] - The password for your bucket
	 * @param {Function} [callback] - The function (in an error first format)
	 *  to be called when the handshake process is completed.
	 * @returns {Bucket}
	 *
	 * @since 2.0.0
	 */
	getBucket( name, password, callback ) {

		let
			connectedBuckets = _buckets.get( this ),
			bucket;

		if ( arguments.length < 3 ) {

			//noinspection JSValidateTypes
			callback = password;
			password = void 0;
		}

		bucket = connectedBuckets ? connectedBuckets.find( b => b._name === name ) : void 0;

		if ( !bucket ) {

			if ( password === void 0 || password === null ) {
				password = this.password;
			}

			//noinspection JSUnresolvedFunction
			bucket = this.connection.openBucket( name, password, ( err ) => {

				if ( err ) {
					return callback( err );
				}

				if ( this.n1qlPort ) {
					bucket.enableN1ql( `http://${this.host}:${this.n1qlPort}` );
				}
				return callback( void 0, bucket );
			} );

		} else {

			callback( void 0, bucket );
		}

		return bucket;
	}


	/**
	 * Triggers an empty query that forces views to index before calling the callback
	 *
	 * @param {string} bucket - The bucket name.
	 * @param {Array.<{design: string, viewQuery: string}>} designAndViews - Views to index.
	 * @param callback
	 * @return {CouchBaseDB}
	 *
	 * @since 2.0.0
	 */
	indexViews( bucket, designAndViews, callback ) {

		if ( designAndViews.constructor !== Array ) {
			designAndViews = [ designAndViews ];
		}

		async.map( designAndViews,
			( view, done ) =>this.query( bucket, ViewQuery
				.from( view.design, view.viewQuery )
				.stale( ViewQuery.Update.BEFORE )
				.include_docs( false )
				.key( [] ), { populate: false }, done ), ( error ) => callback( error ) );

		return this;

	}


	/**
	 *
	 * @param {string} bucket - The name of the bucket to where to look for the document
	 * @param {DocumentKey} key - The unique key of the document
	 * @param {Object} [options]
	 * @param {Function} callback
	 * @returns {CouchBaseDB}
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       bucketName = 'b1';

	 *  // Get a document
	 *  db.get(bucketName, 'user::8bf2346d-52fe-40e7-bfc0-3f5ecf32ec69',
	 *  (err, result) => {
	 *      if ( err ) {
	 *          throw err;
	 *      }
	 *
	 *      console.log(`CAS: ${result.cas}`);
	 *  });
	 *
	 * @since 2.0.0
	 */
	get( bucket, key, options, callback ) {

		const
			settings = {};

		if ( arguments.length < 4 ) {

			//noinspection JSValidateTypes
			callback = options;
			//noinspection JSUnusedAssignment
			options = {};
		} else if ( !options ) {
			//noinspection JSUnusedAssignment
			options = {};
		}

		async.waterfall( [
			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.get( key, settings, callback )

		], createOpCallbackInterceptor( callback ) );

		return this;
	}


	/**
	 *
	 * @param {string} bucket
	 *  The name of the bucket to where to look for the document
	 * @param {Array.<string>} keys
	 *  A set of document unique keys
	 * @param {object} [options]
	 * @param {Bucket.MultiGetCallback} callback
	 * @returns {CouchBaseDB}
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       bucketName = 'b1';

	 *  // Get a document
	 *  db.getMulti(bucketName, [
	 *  'user::8bf2346d-52fe-40e7-bfc0-3f5ecf32ec69',
	 *  'user::03fb7a47-4e48-4fbb-b8f5-b4b590c3eb73',
	 *  'user::92d9db20-9e2f-4bb3-a0a4-52f8bb5e0953'
	 *  ], (errors, result) => {
	 *      if ( errors ) {
	 *          throw errors.find( e => !!e );
	 *      }
	 *
	 *      result.forEach( r =>  console.log(`CAS: ${r.cas}\n`));
	 *  });
	 *
	 * @since 2.0.0
	 */
	getMulti( bucket, keys, options, callback ) {


		if ( arguments.length < 4 ) {

			//noinspection JSValidateTypes
			callback = options;
			//noinspection JSUnusedAssignment
			options = {};
		} else if ( !options ) {
			//noinspection JSUnusedAssignment
			options = {};

		}


		if ( keys && keys.constructor !== Array ) {
			keys = [ keys ];
		}

		async.waterfall( [
			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.getMulti( keys, callback )

		], createMultiGetCallbackInterceptor( callback ) );

		return this;
	}


	/**
	 *
	 * @param {string} bucket
	 *  The name of the bucket to where to look for the document
	 * @param {string} key
	 *  The unique key of the document
	 * @param {object} [options]
	 *  @param {number} [options.lockTime=15]
	 *  @param {boolean} [options.populate=true]
	 * @param {Bucket.OpCallback|CouchBaseDB.OpCallback} callback
	 * @returns {CouchBaseDB}
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       bucketName = 'b1';

	 *  // Get a document
	 *  db.getAndLock(bucketName, 'user::8bf2346d-52fe-40e7-bfc0-3f5ecf32ec69',
	 *  (err, result) => {
	 *      if ( err ) {
	 *          throw err;
	 *      }
	 *
	 *      db.update( bucketName, result.hit, { cas: result.cas }, ( err ) => {
	 *          if ( err ) {
	 *              throw err;
	 *          }
	 *      ));
	 *  });
	 *
	 * @since 2.0.0
	 */
	getAndLock( bucket, key, options, callback ) {

		const
			settings = {};

		if ( arguments.length < 4 ) {

			//noinspection JSValidateTypes
			callback = options;
			options = {};
		} else {

			if ( !options ) {
				options = {};
			}
		}


		if ( options.lockTime === void 0 ) {
			settings.lockTime = 15;
		} else {
			settings.lockTime = options.lockTime;
		}


		if ( options.populate === void 0 ) {
			settings.populate = true;
		} else {
			settings.populate = options.populate;
		}

		async.waterfall( [
			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.getAndLock( key, settings, callback )

		], createOpCallbackInterceptor( callback ) );

		return this;
	}


	/**
	 *
	 * @param bucket
	 * @param key
	 * @param value
	 * @param options
	 * @param callback
	 * @returns {CouchBaseDB}
	 */
	insert( bucket, key, value, options, callback ) {

		const
			_options = {};

		let
			tasks,
			populate,
			views;

		if ( arguments.length < 5 ) {
			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		_options.expiry = options.expiry || 0;
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		populate = options.populate === void 0 ? true : !!options.populate;
		views = options.views;


		tasks = [
			( callback ) =>
				this.getBucket( bucket, callback ),
			( bucket, callback ) =>
				bucket.insert( key, value, _options, callback )
		];


		if ( populate && views ) {

			tasks.push( ( result, callback ) => async.parallel( {
					indexViews: ( done ) => this.indexViews( bucket, views, done ),
					get: ( done ) => this.get( bucket, key, callback )
				},
				( error, results ) => {

					if ( error ) {
						callback( error );
					} else {
						callback( void 0, results.get );
					}
				} )
			);

		} else if ( populate ) {

			tasks.push( ( result, callback ) => this.get( bucket, key, callback ) );
		} else if ( views ) {

			tasks.push( ( result, callback ) => this.indexViews( bucket, views, ( error ) => {

				if ( error ) {
					callback( error );
				} else {
					callback( void 0, result );
				}
			} ) );
		}

		async.waterfall( tasks, callback );

		return this;
	}

	/**
	 *
	 * @param bucket
	 * @param key
	 * @param value
	 * @param options
	 * @param callback
	 * @returns {CouchBaseDB}
	 */
	upsert( bucket, key, value, options, callback ) {

		const
			_options = {};


		let
			tasks,
			populate,
			views;


		if ( arguments.length < 5 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}


		if ( options.cas !== void 0 ) {
			_options.cas = options.cas;
		}

		_options.expiry = options.expiry || 0;
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		populate = options.populate === void 0 ? true : !!options.populate;
		views = options.views;


		tasks = [
			( callback ) =>
				this.getBucket( bucket, callback ),
			( bucket, callback ) =>
				bucket.upsert( key, value, _options, callback )
		];


		if ( populate && views ) {

			tasks.push( ( result, callback ) => async.parallel( {
					indexViews: ( done ) => this.indexViews( bucket, views, done ),
					get: ( done ) => this.get( bucket, key, callback )
				},
				( error, results ) => {

					if ( error ) {
						callback( error );
					} else {
						callback( void 0, results.get );
					}
				} )
			);

		} else if ( populate ) {

			tasks.push( ( result, callback ) => this.get( bucket, key, callback ) );
		} else if ( views ) {

			tasks.push( ( result, callback ) => this.indexViews( bucket, views, ( error ) => {

				if ( error ) {
					callback( error );
				} else {
					callback( void 0, result );
				}
			} ) );
		}

		async.waterfall( tasks, callback );

		return this;
	}


	/**
	 *
	 * @param {string} bucket
	 *  The name of the bucket to where to look for the document
	 * @param {DocumentKey} key
	 *  The unique key of the document to update
	 * @param {Object} value
	 *  The new value for the document
	 * @param {Object} [options]
	 *  @param {Bucket.CAS} [options.cas=undefined]
	 *   The CAS value to check. If the item on the server contains a different
	 *   CAS value, the operation will fail.  Note that if this option is undefined,
	 *   no comparison will be performed.
	 *  @param {number} [options.expiry=0]
	 *   Set the initial expiration time for the document.  A value of 0 represents
	 *   never expiring.
	 *  @param {number} [options.persist_to=0]
	 *   Ensures this operation is persisted to this many nodes.
	 *  @param {number} [options.replicate_to=0]
	 *   Ensures this operation is replicated to this many nodes.
	 *  @param {boolean} [options.populate=undefined]
	 *   If true the document is retrieved after the main operation.
	 *  @param {Object} [options.views=undefined]
	 *   Views to be indexed after the main operation finishes.
	 * @param {Bucket.OpCallback} callback
	 * @returns {CouchBaseDB}
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       bucketName = 'b1';

	 *  // Get a document
	 *  db.getAndLock(bucketName, 'user::8bf2346d-52fe-40e7-bfc0-3f5ecf32ec69',
	 *  (err, result) => {
	 *      if ( err ) {
	 *          throw err;
	 *      }
	 *
	 *      db.replace( bucketName, result.hit, result.value, { cas: result.cas }, ( err ) => {
	 *          if ( err ) {
	 *              throw err;
	 *          }
	 *      ));
	 *  });
	 *
	 * @since 2.0.0
	 */
	replace( bucket, key, value, options, callback ) {

		const
			_options = {};


		let
			tasks,
			populate,
			views;


		if ( arguments.length < 5 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};
		}

		if ( options.cas !== void 0 ) {
			_options.cas = options.cas;
		}

		_options.expiry = options.expiry || 0;
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		populate = options.populate === void 0 ? true : !!options.populate;
		views = options.views;


		tasks = [
			( callback ) =>
				this.getBucket( bucket, callback ),
			( bucket, callback ) =>
				bucket.upsert( key, value, _options, callback )
		];


		if ( populate && views ) {

			tasks.push( ( result, callback ) => async.parallel( {
					indexViews: ( done ) => this.indexViews( bucket, views, done ),
					get: ( done ) => this.get( bucket, key, callback )
				},
				( error, results ) => {

					if ( error ) {
						callback( error );
					} else {
						callback( void 0, results.get );
					}
				} )
			);

		} else if ( populate ) {

			tasks.push( ( result, callback ) => this.get( bucket, key, callback ) );
		} else if ( views ) {

			tasks.push( ( result, callback ) => this.indexViews( bucket, views, ( error ) => {

				if ( error ) {
					callback( error );
				} else {
					callback( void 0, result );
				}
			} ) );
		}

		async.waterfall( tasks, callback );

		return this;
	}


	/**
	 *
	 * @param {string} bucket
	 *  The name of the bucket to where to look for the document
	 * @param {DocumentKey} key
	 *  The unique key of the document to remove
	 * @param {object} [options]
	 *  @param {object} [options.views=undefined]
	 *   Views to be indexed after the main operation finishes.
	 * @param {Bucket.OpCallback} callback
	 * @returns {CouchBaseDB}
	 * @example
	 *
	 *  const
	 *       db = new CouchBaseDB( { host: 'localhost', password: 'ruler' } ),
	 *       bucketName = 'b1';

	 *  // Get a document
	 *  db.getAndLock(bucketName, 'user::8bf2346d-52fe-40e7-bfc0-3f5ecf32ec69',
	 *  (err, result) => {
	 *      if ( err ) {
	 *          throw err;
	 *      }
	 *
	 *      db.replace( bucketName, result.hit, result.value, { cas: result.cas }, ( err ) => {
	 *          if ( err ) {
	 *              throw err;
	 *          }
	 *      ));
	 *  });
	 *
	 * @since 2.0.0
	 */
	remove( bucket, key, options, callback ) {

		const
			_options = {};


		let
			tasks,
			views;

		if ( arguments.length < 4 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};

		}

		if ( options.cas !== void 0 ) {
			_options.cas = options.cas;
		}

		_options.expiry = options.expiry || 0;
		_options.persist_to = options.persist_to || 0;
		_options.replicate_to = options.replicate_to || 0;

		views = options.views;


		tasks = [

			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.remove( key, _options, callback )
		];


		if ( views ) {
			tasks.push( ( data, callback ) =>
				this.indexViews( bucket, _options.views, ( error ) => callback( error, data ) ) );
		}

		async.waterfall( tasks, callback );

		return this;
	}


	query( bucket, query, options, callback ) {

		let
			_options = {};


		if ( arguments.length < 4 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};

		}


		if ( query === void 0 || query === null ) {
			throw new TypeError( 'Query must be an object or a string.' );
		}


		if ( query.constructor === CouchBaseDB.ViewQuery ) {

			if ( options.populate === void 0 ) {
				_options.populate = true;
			} else {
				_options.populate = options.populate || false;
			}

		} else if ( query.constructor === String ) {

			_options = options;
			//noinspection JSUnresolvedFunction
			query = N1qlQuery.fromString( query );
		}


		async.waterfall( [

			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) => bucket.query( query, _options, callback ),

			( results, meta, callback ) => _options.populate === true && results.length ?
				this.getMulti( bucket, results.map( r => r.id ), options, callback ) : callback( void 0, results )

		], callback );

		return this;
	}


	counter( bucket, key, value, options, callback ) {

		if ( arguments.length < 5 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};

		}


		if ( options.initial === void 0 ) {
			options.initial = 0;
		}

		async.waterfall( [

			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.counter( key, value, options, callback )

		], callback );

		return this;
	}


	unlock( bucket, key, cas, options, callback ) {

		if ( arguments.length < 5 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};

		}


		async.waterfall( [
			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.unlock( key, cas, options, callback )

		], callback );

		return this;
	}


	touch( bucket, key, expiry, options, callback ) {

		if ( arguments.length < 5 ) {

			callback = options;
			options = {};
		} else if ( !options ) {
			options = {};

		}


		async.waterfall( [
			( callback ) =>
				this.getBucket( bucket, callback ),

			( bucket, callback ) =>
				bucket.unlock( key, expiry, options, callback )

		], callback );

		return this;
	}

}


//noinspection JSUnresolvedVariable
module.exports = CouchBaseDB;