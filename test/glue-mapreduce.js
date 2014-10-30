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
} );
