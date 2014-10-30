var _       = require( 'lodash' ),
    assert  = require( 'assert' ),
    expect  = require( 'chai' ).expect,
    path    = require( 'path' ),
    rootDir = path.join( '.', '..' );

var NotImplementError = require( rootDir, 'lib', 'errors' ).NotImplementError;
var GlueMapReduce = require( path.join( rootDir, 'lib', 'glue-mapreduce' ) );

describe( 'GlueMapReduce', function () {
    var mr = null;
    beforeEach( function ( done ) {
        mr = new GlueMapReduce();
        done();
    } );

    describe( 'mapper', function () {
        it( 'mapper function not implemented', function ( done ) {
            expect( mr.mapper ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'reducer', function () {
        it( 'reducer function not implemented', function ( done ) {
            expect( mr.mapper ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'input', function () {
        it( 'input function not implemented', function ( done ) {
            expect( mr.input ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'run', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
        it( 'input function not implemented', function ( done ) {
            expect( mr.run ).to.throw( NotImplementError );
            done();
        } );

        it( 'input function return error', function ( done ) {
            mr.input = function ( callback ) {
                return callback( 'EEEEEEEERRRRORoRO0RRR' );
            };
            mr.run( function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( 'input function return not iterable data', function ( done ) {
            mr.input = function ( callback ) {
                var notIterable = 98424;
                return callback( null, notIterable );
            };
            mr.run( function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( 'mapper mode should be run _runHadoopMapper', function ( done ) {
            mr.mode = 'mapper';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runHadoopMapper = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'map mode should be run _runHadoopMapper', function ( done ) {
            mr.mode = 'map';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runHadoopMapper = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'reducer mode should be run _runHadoopReducer', function ( done ) {
            mr.mode = 'reducer';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runHadoopReducer = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'reduce mode should be run _runHadoopReducer', function ( done ) {
            mr.mode = 'reduce';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runHadoopReducer = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'red mode should be run _runHadoopReducer', function ( done ) {
            mr.mode = 'red';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runHadoopReducer = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'Default should be run _runLocal', function ( done ) {
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runLocal = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'Invalid mode should be run _runLocal', function ( done ) {
            mr.mode = 'aaaaaaaaaaaaaaaaaaaaaa';
            mr.input = function ( callback ) {
                return callback( null, [] );
            };
            mr._runLocal = function ( data, callback ) {
                assert.deepEqual( data, [] );
                return callback();
            };

            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );
    } );

    describe( 'shuffler', function () {
        it( 'Same key must be merged', function ( done ) {
            var data = [
                { k: 'hoge', v: 1 },
                { k: 'hoge', v: 2 }
            ];
            mr.shuffler( data, function ( error, result ) {
                var expect = [ { hoge: [ 1, 2 ] } ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );

        it( 'Defferent key must be splited', function ( done ) {
            var data = [
                { k: 'hoge', v: 1 },
                { k: 'fuga', v: 2 }
            ];
            mr.shuffler( data, function ( error, result ) {
                var expect = [
                    { hoge: [ 1 ] },
                    { fuga: [ 2 ] }
                ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );

        it( 'Not objected data', function ( done ) {
            var data = [
                'aaaaaaaaaaaaa',
                { k: 'hoge', v: 2 }
            ];
            mr.shuffler( data, function ( error, result ) {
                var expect = [ { hoge: [ 2 ] } ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );

        it( 'Not have k field should return undefined key', function ( done ) {
            var data = [
                { hoge: 'a', v: 2 },
                { fuga: 'b', v: 4 }
            ];
            mr.shuffler( data, function ( error, result ) {
                var expect = [ { 'undefined': [ 2, 4 ] } ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );

        it( 'Not have v field should return undefined value', function ( done ) {
            var data = [
                { k: 'a', hoge: 1 },
                { k: 'a', hoge: 1 },
            ];
            mr.shuffler( data, function ( error, result ) {
                var expect = [ { a: [ void(0), void(0) ] } ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );
    } );

    describe( '_applyLocalMapper', function () {
        it( 'mapper returns error', function ( done ) {
            mr.mapper = function ( data, callback ) {
                return callback( 'error' );
            };
            mr._applyLocalMapper( [], function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mapper returns key value objects', function ( done ) {
            var results = [
                { k: 'this', v: 'is' },
                { k: 'results', v: 'objects' }
            ];
            mr.mapper = function ( data, callback ) {
                return callback( null, results );
            };
            mr._applyLocalMapper( [ 'something' ], function ( error, keyValueObjects ) {
                assert.equal( error, null );
                assert.deepEqual( keyValueObjects, results );
                done();
            } );
        } );
    } );

    describe( '_runLocal', function () {
        it( '_applyLocalMapper returns error', function ( done ) {
            mr._applyLocalMapper = function ( data, callback ) {
                return callback( 'errororroorrrrrr' );
            };
            mr._runLocal( [], function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( 'shuffler returns error', function ( done ) {
            mr.shuffler = function ( data, callback ) {
                return callback( 'errororroorrrrrr' );
            };
            mr._runLocal( [], function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( '_applyLocalReducer returns error', function ( done ) {
            mr._applyLocalReducer = function ( data, callback ) {
                return callback( 'errororroorrrrrr' );
            };
            mr._runLocal( [], function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( 'results must be ' );
    } );
} );
