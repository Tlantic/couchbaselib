"use strict";
const
	uuid = require('uuid'),
	async = require('async'),
	couchbase = require( 'couchbase' ).Mock,
	CouchbaseDB = require( '../lib/couchbasedb' )( couchbase ),
	expect = require( 'chai' ).expect;


describe( 'CouchbaseDB', function () {

	function releaseBuckets(cluster, callback) {
		return function() {
			cluster.release();
			callback();
		};
	}

	it( 'Should expose query classes', function(done) {
		expect(CouchbaseDB.ViewQuery).to.be.a.function;
		expect(CouchbaseDB.N1qlQuery).to.be.a.function;
		done();
	} );

	describe('#new CouchbaseDB()', function() {

		let
			defaults = {
				host: "localhost",
				adminPort: 8091,
				apiPort: 8092,
				sslAdminPort: 18091,
				sslApiPort: 18092,
				password: null,
				n1qlPort: 8093
			};


		it( 'Should initiate with default connection settings', function ( done ) {


				const
					customSetting = 'foobar';

				for ( const propNameX in defaults ) {

					if ( defaults.hasOwnProperty(propNameX) ) {

						const
							cluster = new CouchbaseDB({ [propNameX]: customSetting });

						for ( const propNameY in defaults ) {
							if (defaults.hasOwnProperty(propNameY)) {

								if ( propNameY === propNameX ) {
									expect( cluster ).have.property( propNameY, customSetting);
								} else {
									expect( cluster ).have.property( propNameY, defaults[propNameY]);
								}

							}
						}

					}

				}


			done();
		} );


		describe('.initConnection()', function() {
			const
				cluster = new CouchbaseDB();

			it ('should return a cluster instance', function(done) {
				let
					clusterConnection = cluster.initConnection( );

				expect(clusterConnection).to.be.instanceOf(couchbase.Cluster);
				expect(clusterConnection.dsnObj).to.be.a.object;
				expect(clusterConnection.dsnObj.hosts).to.be.a.array;
				expect(clusterConnection.dsnObj.hosts[0][0]).to.be.equal(defaults.host);
				expect(clusterConnection.dsnObj.hosts[0][1]).to.be.equal(defaults.adminPort);
				done();
			});

			it ('should accept host, password, adminPort, apiPort, sslAdminPort, sslApiPort as arguments ', function(done) {
				let
					clusterConnection = cluster.initConnection('127.0.0.0', 'admin', '88', '80', '8888', '8080' );

				expect(clusterConnection).to.be.instanceOf(couchbase.Cluster);
				expect(clusterConnection.dsnObj).to.be.a.object;
				expect(clusterConnection.dsnObj.hosts).to.be.a.array;


				expect(cluster.host).to.be.equal('127.0.0.0');
				expect(cluster.adminPort).to.be.equal('88');
				expect(cluster.apiPort).to.be.equal('80');
				expect(cluster.sslAdminPort).to.be.equal('8888');
				expect(cluster.sslApiPort).to.be.equal('8080');

				expect(clusterConnection.dsnObj.hosts[0][0]).to.be.equal(cluster.host);
				expect(clusterConnection.dsnObj.hosts[0][1]).to.be.equal(Number(cluster.adminPort));

				done();
			});

		});

		describe('.connection', function() {
			const
				cluster = new CouchbaseDB({n1qlPort: null});

			it ('should be the result of initConnection()', function(done) {
				expect(cluster).to.not.have.ownProperty('connection');
				expect(cluster.connection).to.be.deep.equal(cluster.initConnection());
				done();
			});

		});

		describe('.getBucket', function() {
			const
				cluster = new CouchbaseDB({n1qlPort: null});

			let firstBucket;

			it ('should return a bucket instance', function(done) {
				let
					bucket = firstBucket = cluster.getBucket('bucketName');

				expect(bucket).to.be.instanceOf((new couchbase.Cluster()).openBucket('bucketName').constructor);
				done();
			});

		});

		describe('.insert', function() {
			const
				staticKey = uuid.v4(),
				cluster = new CouchbaseDB({n1qlPort: null});

			it ('should insert a document', function(done) {

				const
					body = { firstName: 'foo', lastName: 'bar' };

				async.series([

					( done )=>cluster.insert('default', uuid.v4(), body, {populate: false},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.not.exist;
						done();
					}),

					( done ) => cluster.insert('default', uuid.v4(), body, {populate: true},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body);
						done();
					})

				], releaseBuckets(cluster, done));

			});

			it (`should return error code ${couchbase.errors.keyAlreadyExists} if a document with same key exists`, function(done) {
				const
					body = { firstName: 'foo', lastName: 'bar' };

				async.series([

					( done )=>cluster.insert('default', staticKey, body, {populate: false},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						done();
					}),
					( done ) => cluster.insert('default', staticKey, {}, ( error, result ) => {
						expect(error).to.exist;
						expect(result).to.not.exist;
						expect(error.code).to.equal(couchbase.errors.keyAlreadyExists);
						done();
					})

				], releaseBuckets(cluster, done))
			});

			it ('should operate on designated bucket', function(done) {

				const
					body_1 = { firstName: 'foo', lastName: 'bar' },
					body_2 = { firstName: 'foozy', lastName: 'barz' };

				async.series( [

					(done) => cluster.insert('default', staticKey, body_1, {populate: true},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body_1);
						done();
					}),

					( done ) => cluster.insert('default_2', staticKey, body_2, {populate: true},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body_2);
						done();
					})

				], releaseBuckets(cluster, done));

			});

		});

		describe('.append', function() {
			const
				cluster = new CouchbaseDB({n1qlPort: null});


			it ('should append text to document', function(done) {

				const
					base = 'foo',
					extent = ' bar',
					docKey = "user::7c3cb8c2-23eb-40d1-bdc4-656211162fad";

				cluster.insert('default', docKey, base, ( error ) => {
					expect(error).to.not.exist;
					cluster.append('default', docKey, extent, ( error, result ) => {
						expect(error).to.not.exist;
						expect(result.value).to.be.equal(base+extent);
						done();
					});
				});


			});

		});


		describe('.get', function() {
			const
				cluster = new CouchbaseDB({n1qlPort: null});

			it ('should retrieve a document', function(done) {

				const
					staticKey = uuid.v4(),
					body = { firstName: 'foo', lastName: 'bar' };

				async.series([

					( done )=>cluster.insert('default', staticKey, body, {populate: false},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.not.exist;
						done();
					}),

					( done ) => cluster.get('default', staticKey,( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body);
						done();
					}),
					( done ) => cluster.get('default', staticKey,( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body);
						done();
					})

				], releaseBuckets(cluster, done));

			});

		});

		describe('.getAndLock', function() {
			const
				cluster = new CouchbaseDB({n1qlPort: null});

			it ('should retrieve and lock a document', function(done) {

				const
					staticKey = uuid.v4(),
					body = { firstName: 'foo', lastName: 'bar' };

				async.series([

					( done )=>cluster.insert('default', staticKey, body, {populate: false},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.not.exist;
						done();
					}),

					( done ) => cluster.getAndLock('default', staticKey, ( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body);

						cluster.get('default', staticKey, ( error, result ) => {
							expect(error).to.exist;
							expect(result.value).to.not.exist;
							expect(error.code).to.equal(couchbase.errors.keyNotFound);
							done();
						});
					})


				], releaseBuckets(cluster, done));

			});

			it ('should lock until specified lockTime reached', function(done) {

				const
					staticKey = uuid.v4(),
					body = { firstName: 'foo', lastName: 'bar' };

				async.series([

					( done )=>cluster.insert('default', staticKey, body, {populate: false},( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.not.exist;
						done();
					}),

					( done ) => cluster.getAndLock('default', staticKey, {lockTime: 100}, ( error, result ) => {
						expect(error).not.to.exist;
						expect(result.cas).to.be.a.object;
						expect(result.value).to.be.deep.equal(body);

						setTimeout( () => cluster.get('default', staticKey, ( error, result ) => {
							expect(error).to.exist;
							expect(result.value).to.not.exist;
							expect(error.code).to.equal(couchbase.errors.keyNotFound);
							done();
						}), 99 );

					})


				], releaseBuckets(cluster, done));

			});

		});
	});


	
});