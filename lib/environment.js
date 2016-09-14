'use strict';

const
	Ajv = require( 'ajv' ),
	CouchBaseDB = require( './couchbasedb' ),
	Model = require( './model' ),

	_defaultEnvOptions = {
		$async: true,
		useDefault: "shared",
		useCoerce: true,
		checkRequired: true,
		removeAdditional: true
	};





/**
 * CouchBase execution environment.
 * Provides access to
 *
 * @param {Object} [options] - CouchBaseDB constructor options.
 * @returns {CouchBaseEnvironment}
 *
 * @class CouchBaseEnvironment
 * @extends CouchBaseDB
 * @since 2.0.0
 */
class CouchBaseEnvironment extends CouchBaseDB {


	/**
	 * Convenience getter for class Module
	 */
	static get Model() {
		return Model;
	}


	constructor( options ) {

		const
			envSettings = {};

		super( options );

		if ( !options ) {
			options = {};
		}

		for ( const propName in _defaultEnvOptions ) {
			if ( _defaultEnvOptions.hasOwnProperty( propName ) ) {
				envSettings[ propName ] = options.hasOwnProperty( propName ) ?
					options[ propName ] : _defaultEnvOptions[ propName ]
			}
		}

		this.validator = new Ajv( envSettings );

		this.models = new Map();

	}


	//noinspection JSMethodCanBeStatic
	/**
	 * Convenience class circular reference.
	 *
	 * @see CouchBaseEnvironment
	 * @since 2.0.0
	 */
	get CouchBaseEnvironment() {
		return CouchBaseEnvironment;
	}


	//noinspection JSMethodCanBeStatic
	/**
	 * Convenience getter for class Module
	 */
	get Model() {
		return Model;
	}


	/**
	 * Model factory method.
	 * Acts as a proxy for getModel and and createModel, calling
	 * the appropriate method based on arguments length
	 *
	 * NOTE: If only the model name is passed as argument and
	 * the custom model is unknown in this environment an Error
	 * will be thrown.
	 *
	 * @param {string} name - The model name.
	 * @returns {Model}
	 *
	 * @see CouchBaseEnvironment~getModel
	 * @see CouchBaseEnvironment~createModel
	 * @since 2.0.0
	 */
	model( name ) {
		if ( arguments.length < 2 ) {
			return this.getModel.apply( this, arguments );
		}
		return this.createModel.apply( this, arguments );
	}


	/**
	 * Returns a previously defined model.
	 *
	 * @param {string} name - Model name.
	 * @returns {Model}
	 *
	 * @since 2.0.0
	 */
	getModel( name ) {
		if ( this.models.has( name ) ) {
			return this.models.get( name );
		} else {
			throw new Error( `Model not found: ${name}` );
		}
	}

	/**
	 *
	 * @param {Bucket} bucket
	 * @param {string} name
	 * @param {Object|string} schema
	 * @param {Object} [methods]
	 * @param {Object} [prototype]
	 */
	createModel( bucket, name, schema, methods, prototype ) {

		const
			db = this,

			/**
			 * @class TypeModel
			 * @classdesc
			 *  Extended Model class targeting a specific schema
			 *  with custom functions and methods.
			 *
			 * @extends Model
			 * @since 2.0.0
			 */
			TypeModel = class extends Model {

				//noinspection JSUnusedGlobalSymbols
				static get name() {
					return name;
				}

				//noinspection JSUnusedGlobalSymbols
				static get type() {
					return name;
				}

				//noinspection JSUnusedGlobalSymbols
				static get bucket() {
					return bucket
				}

				//noinspection JSUnusedGlobalSymbols
				/**
				 * Constructs a unique key for the document.
				 *
				 * @param {DocumentUUID} uuid - A unique id within the given type.
				 * @returns {DocumentKey} - The id of the document.
				 */
				static modelKey( uuid ) {
					return `${name}::${uuid}`;
				}


				//noinspection JSUnusedGlobalSymbols
				/**
				 * @constructs Type Model
				 * @param {Object|String} [data] Members or uId.
				 *
				 * @since 2.0.0
				 */
				constructor( data ) {

					if ( !data || data.constructor === String ) {

						super( name, data );

					} else {

						super( name );

						for ( const propName in data ) {
							if ( data.hasOwnProperty( propName ) ) {
								this[ propName ] = data[ propName ];
							}
						}
					}
				}

			};



		if ( schema.constructor === Object ) {

			const
				meta = {
					_uId: {
						type: "string"
					},
					_type: {
						type: "string"
					},
					_createDate: {
						type: "integer"
					},
					_updateDate: {
						type: "integer"
					},
					_createUser: {
						type: "string",
						default: ""
					},
					_updateUser: {
						type: "string",
						default: ""
					}
				};

			let required = ['_uId', '_type', '_createDate', '_updateDate'];

			if ( !schema.properties ) {

				schema.properties = meta;
				schema.required = required;
			} else {

				for ( const propName in meta ) {
					if ( meta.hasOwnProperty( propName ) ) {
						schema.properties[ propName ] = meta[ propName ];
					}
				}
			}

			if ( schema.required ) {
				schema.required = required.concat(schema.required);
			}
		}

		/* append class methods to class */
		if ( methods ) {
			for ( const propName in methods ) {
				if ( methods.hasOwnProperty( propName ) && !TypeModel[ propName ] )
					TypeModel[ propName ] = methods[ propName ];
			}
		}

		/* append prototype methods to class prototype */
		if ( prototype ) {
			for ( const propName in prototype ) {
				if ( prototype.hasOwnProperty( propName ) )
					TypeModel.prototype[ propName ] = prototype[ propName ];
			}
		}

		/* register schema */
		if ( schema.$async !== false ) {
			schema.$async = true;
		}
		db.validator.addSchema( schema, name );

		/* register model */
		this.models.set( name, TypeModel );

		TypeModel.setContext( db, bucket );

		//noinspection JSDuplicatedDeclaration
		let validate = db.validator.compile( schema );


		if ( schema.$async === true ) {
			TypeModel.validate = ( obj, callback ) =>
				validate(obj)
					.then(() => callback(void 0, obj))
					.catch((e) => callback(e));

		} else {
			TypeModel.validate = ( obj, callback ) => {
				if ( validate(obj) ) {
					callback(void 0, obj);
				} else {
					callback(validate.errors);
				}
			};
		}

	}
}

module.exports = CouchBaseEnvironment;

