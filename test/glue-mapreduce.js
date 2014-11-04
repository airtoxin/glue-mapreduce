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
            expect( mr.reducer ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'input', function () {
        it( 'Default input function returns empty array', function ( done ) {
            mr.input( function ( error, data ) {
                assert.equal( error, null );
                assert.deepEqual( data, [] );
                done();
            } );
        } );
    } );

    describe( '_input', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
        it( 'mapper mode is Stdin mode', function ( done ) {
            mr.mode = 'mapper';
            mr._getStdin = function () {
                return done();
            };
            mr.input = function () {
                assert.ok( false, 'input function should not called' );
            };
            mr._input();
        } );
        it( 'map mode is Stdin mode', function ( done ) {
            mr.mode = 'map';
            mr._getStdin = function () {
                return done();
            };
            mr.input = function () {
                assert.ok( false, 'input function should not called' );
            };
            mr._input();
        } );
        it( 'reducer mode is Stdin mode', function ( done ) {
            mr.mode = 'reducer';
            mr._getStdin = function () {
                return done();
            };
            mr.input = function () {
                assert.ok( false, 'input function should not called' );
            };
            mr._input();
        } );
        it( 'reduce mode is Stdin mode', function ( done ) {
            mr.mode = 'reduce';
            mr._getStdin = function () {
                return done();
            };
            mr.input = function () {
                assert.ok( false, 'input function should not called' );
            };
            mr._input();
        } );
        it( 'red mode is Stdin mode', function ( done ) {
            mr.mode = 'red';
            mr._getStdin = function () {
                return done();
            };
            mr.input = function () {
                assert.ok( false, 'input function should not called' );
            };
            mr._input();
        } );
        it( 'Default mode is input mode', function ( done ) {
            mr.mode = 'aaaaa';
            mr._getStdin = function () {
                assert.ok( false, '_getStdin function should not called' );
            };
            mr.input = function () {
                return done();
            };
            mr._input();
        } );
    } );

    describe( 'run', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );

        it( 'error occurred', function ( done ) {
            var flag = false;
            mr._input = function ( callback ) {
                flag = true;
                return callback( 'eeeerrreeeroooorrrrr' );
            };
            mr.run( function ( error ) {
                assert.notEqual( error, null );
                assert.ok( flag );
                done();
            } );
        } );

        it( 'data is not array', function ( done ) {
            var flag = false;
            mr._input = function ( callback ) {
                flag = true;
                return callback( null, 892498 );
            };
            mr.run( function ( error ) {
                assert.notEqual( error, null );
                assert.ok( flag );
                done();
            } );
        } );

        it( 'mode is mapper', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runHadoopMapper = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
                return callback();
            };
            mr.mode = 'mapper';
            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mode is map', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runHadoopMapper = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
                return callback();
            };
            mr.mode = 'map';
            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mode is reducer', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runHadoopReducer = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
                return callback();
            };
            mr.mode = 'reducer';
            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mode is reduce', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runHadoopReducer = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
                return callback();
            };
            mr.mode = 'reduce';
            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mode is red', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runHadoopReducer = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
                return callback();
            };
            mr.mode = 'red';
            mr.run( function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'mode is default', function ( done ) {
            var data = [ { k: 'key', v: 'vaaaaaalue' } ];
            mr._input = function ( callback ) {
                return callback( null, data );
            };
            var flag = false;
            mr._runLocal = function ( input, callback ) {
                flag = true;
                assert.deepEqual( input, data );
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
                var expect = [ { k: 'hoge', v: [ 1, 2 ] } ];
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
                    { k: 'hoge', v: [ 1 ] },
                    { k: 'fuga', v: [ 2 ] }
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
                var expect = [ { k: 'hoge', v: [ 2 ] } ];
                assert.deepEqual( result, expect );
                done();
            } );
        } );

        it( 'Not have k field', function ( done ) {
            var data = [
                { hoge: 'a', v: 2 },
                { fuga: 'b', v: 4 }
            ];
            mr.shuffler( data, function ( error, result ) {
                assert.deepEqual( result, [] );
                done();
            } );
        } );

        it( 'Not have v field', function ( done ) {
            var data = [
                { k: 'a', hoge: 1 },
                { k: 'a', hoge: 1 },
            ];
            mr.shuffler( data, function ( error, result ) {
                assert.deepEqual( result, [] );
                done();
            } );
        } );
    } );

    describe( '_applyLocalMapper', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
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
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
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

        it( 'Map-Reduce results', function ( done ) {
            var input = [
                'key1\tv',
                'key1\tvv',
                'key2\tvvv'
            ];
            mr.mapper = function ( line, callback ) {
                var s = line.split( '\t' );
                return callback( null, { k: s[0], v: s[1] } );
            };
            mr.reducer = function ( key, values, callback ) {
                return callback( null, [ { k: key, v: values.length } ] );
            };
            // defined mapreduce is word counter of key
            var expect = [
                { k: 'key1', v: 2 },
                { k: 'key2', v: 1 }
            ];
            mr._runLocal( input, function ( error, results ) {
                assert.equal( error, null );
                assert.deepEqual( results, expect );
                done();
            } );
        } );
    } );

    describe( '_runHadoopMapper', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
        it( 'mapper called per line', function ( done ) {
            var counter = 0;
            mr.mapper = function ( line, callback ) {
                counter++;
                return callback();
            };
            mr._runHadoopMapper( [ 'a', 'b', 'c' ], function ( error ) {
                assert.equal( error, null );
                assert.equal( counter, 3 );
                done();
            } );
        } );

        it( 'mapper returns error', function ( done ) {
            var counter = 0;
            mr.mapper = function ( line, callback ) {
                counter++;
                return callback( 'EOOOERRRRRRRR' );
            };
            mr._runHadoopMapper( [ 'a', 'b', 'c' ], function ( error ) {
                assert.notEqual( error, null );
                assert.equal( counter, 1 );
                done();
            } );
        } );

        it( '_outMapResults called per line', function ( done ) {
            mr.mapper = function ( line, callback ) {
                return callback( null );
            };
            var counter = 0;
            mr._outMapResults = function () {
                counter++;
            };
            mr._runHadoopMapper( [ 'a', 'b', 'c' ], function ( error ) {
                assert.equal( error, null );
                assert.equal( counter, 3 );
                done();
            } );
        } );
    } );

    describe( '_outMapResults', function () {
        it( 'synchronous', function ( done ) {
            mr._outMapResults();
            done();
        } );
    } );

    describe( '_outMapResults', function () {
        var systemLog = null;
        beforeEach( function ( done ) {
            systemLog = console.log;
            done();
        } );

        it( 'log out per line', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outMapResults( [ { k: 1, v: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 2 );

            console.log = systemLog
            done();
        } );

        it( 'skip not have k key data', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outMapResults( [ { aaaaaa: 1, v: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 1 );

            console.log = systemLog
            done();
        } );

        it( 'skip not have v key data', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outMapResults( [ { k: 1, aaaaaaaa: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 1 );

            console.log = systemLog
            done();
        } );

        afterEach( function ( done ) {
            console.log = systemLog;
            done();
        } );
    } );

    describe( '_outReduceResults', function () {
        var systemLog = null;
        beforeEach( function ( done ) {
            systemLog = console.log;
            done();
        } );

        it( 'log out per line', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outReduceResults( [ { k: 1, v: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 2 );

            console.log = systemLog
            done();
        } );

        it( 'skip not have k key data', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outReduceResults( [ { aaaaaa: 1, v: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 1 );

            console.log = systemLog
            done();
        } );

        it( 'skip not have v key data', function ( done ) {
            var counter = 0;
            console.log = function () {
                counter++;
            };
            mr._outReduceResults( [ { k: 1, aaaaaaaa: 2 }, { k: 2, v: 3 } ] );
            assert.equal( counter, 1 );

            console.log = systemLog
            done();
        } );

        afterEach( function ( done ) {
            console.log = systemLog;
            done();
        } );
    } );

    describe( '_runHadoopReducer', function () {
        beforeEach( function ( done ) {
            mr = new GlueMapReduce();
            done();
        } );
        it( 'empty data', function ( done ) {
            mr._runHadoopReducer( [], function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'reducer gets values', function ( done ) {
            var flag = false;
            mr.reducer = function ( key, values, callback ) {
                assert.equal( key, 'key' );
                assert.deepEqual( values, [ 'a', 'b', 'c' ] );
                flag = true;
                return callback();
            };
            var data = [
                'key\ta',
                'key\tb',
                'key\tc'
            ];
            mr._runHadoopReducer( data, function ( error ) {
                assert.equal( error, null );
                done();
            } );
        } );

        it( 'reducer returns error', function ( done ) {
            mr.reducer = function ( key, values, callback ) {
                return callback( 'this is error' );
            };
            var data = [
                'key\ta',
                'key\tb',
                'key2\ta',
                'key2\tb'
            ];
            mr._runHadoopReducer( data, function ( error ) {
                assert.notEqual( error, null );
                done();
            } );
        } );

        it( '_outReduceResults run', function ( done ) {
            var data = [
                'key\ta',
                'key\tb',
                'key2\tc',
                'key2\td'
            ];
            var expect = [
                { k: 'key',  v: 'ab' },
                { k: 'key2', v: 'cd' }
            ];
            var flag = false;
            mr._outReduceResults = function ( keyValueObjects ) {
                flag = true;
            };
            mr.reducer = function ( key, values, callback ) {
                return callback( null, [ { k: key, v: values.join('') } ] );
            };
            mr._runHadoopReducer( data, function ( error ) {
                assert.ok( flag );
                assert.equal( error, null );
                done();
            } );
        } )
    } );
} );
