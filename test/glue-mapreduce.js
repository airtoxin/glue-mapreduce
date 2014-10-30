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
        it( 'mapper function Not Implemented', function ( done ) {
            expect( mr.mapper ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'reducer', function () {
        it( 'reducer function Not Implemented', function ( done ) {
            expect( mr.mapper ).to.throw( NotImplementError );
            done();
        } );
    } );

    describe( 'run', function () {
        it( 'mapper mode should be run _runHadoopMapper', function ( done ) {
            mr.mode = 'mapper';
            mr._runHadoopMapper = function () {
                return done();
            };

            mr.run();
        } );

        it( 'map mode should be run _runHadoopMapper', function ( done ) {
            mr.mode = 'map';
            mr._runHadoopMapper = function () {
                return done();
            };

            mr.run();
        } );

        it( 'reducer mode should be run _runHadoopReducer', function ( done ) {
            mr.mode = 'reducer';
            mr._runHadoopReducer = function () {
                return done();
            };

            mr.run();
        } );

        it( 'map mode should be run _runHadoopReducer', function ( done ) {
            mr.mode = 'reducer';
            mr._runHadoopReducer = function () {
                return done();
            };

            mr.run();
        } );

        it( 'Default should be run _runLocal', function ( done ) {
            mr._runLocal = function () {
                return done();
            };

            mr.run();
        } );

        it( 'Invalid mode should be run _runLocal', function ( done ) {
            mr.mode = 'aaaaaaaaaaaaaaaaaaaaaa';
            mr._runLocal = function () {
                return done();
            };

            mr.run();
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
} );
