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
		removeAdditional: true,
		ownProperties: true,
		additionalProperties: false
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
	 * @param {string} bucket
	 * @param {string} name
	 * @param {Object|null|undefined} [schema]
	 * @param {Object|null|undefined} [methods]
	 * @param {Object|null|undefined} [prototype]
	 * @param {Object|null|undefined} [metaProperties]
	 */
	createModel( bucket, name, schema, methods, prototype, metaProperties ) {

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



		TypeModel.setContext( db, bucket, metaProperties );


		/* register schema */
		if ( schema ) {
			
			let
				validate;

			if ( schema.$async === void 0 ) {
				schema.$async = true;
			}

			if ( schema.additionalProperties === void 0 ) {
				schema.additionalProperties = false;
			}

			//noinspection JSUnresolvedFunction
			validate = db.validator.compile( schema );
			//noinspection JSUnresolvedFunction
			db.validator.addSchema( schema, name );
			
			
			//noinspection JSUnresolvedVariable
			if ( schema.$async === true ) {
				TypeModel.validate = function _validate( obj, callback ) {
					validate( obj )
						.then( function () {
							return callback( void 0, obj )
						} )
						.catch( function ( e ) {
							return callback( e )
						} )
				}
			} else {
				TypeModel.validate = function _validate( obj, callback ) {
					if ( validate( obj ) ) {
						callback( void 0, obj );
					} else {
						callback( validate.errors );
					}
				};
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


		/* register model */
		this.models.set( name, TypeModel );

	}
}

module.exports = CouchBaseEnvironment;

