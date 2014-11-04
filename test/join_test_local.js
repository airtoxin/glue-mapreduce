var _       = require( 'lodash' ),
    assert  = require( 'assert' ),
    path    = require( 'path' ),
    rootDir = path.join( '.', '..' );

var GlueMapReduce = require( path.join( rootDir, 'lib', 'glue-mapreduce' ) );

describe( 'join test local', function () {
    var mr   = null;
    var data = null;
    beforeEach( function ( done ) {
        mr = new GlueMapReduce();
        data = null;
        mr.input = function ( callback ) {
            return callback( null, data );
        };
        mr.mapper = function ( line, callback ) {
            var split = line.split( ' ' );
            var data = _.map( split, function ( word ) {
                return { k: word, v: 1 };
            } );
            return callback( null, data );
        };
        mr.reducer = function ( key, values, callback ) {
            return callback( null, [ { k: key, v: values.length } ] );
        };
        mr.mode = 'local';
        done();
    } );

    it( 'Aggregate', function ( done ) {
        data = [
            'a',
            'a b',
            'a b c',
            'a b c d',
            'a b c d e',
            'a b c d e f',
            'a b c d e f g'
        ];
        var expect = [
            { k: 'a', v: 7 },
            { k: 'b', v: 6 },
            { k: 'c', v: 5 },
            { k: 'd', v: 4 },
            { k: 'e', v: 3 },
            { k: 'f', v: 2 },
            { k: 'g', v: 1 },
        ];
        mr.run( function ( error, result ) {
            assert.equal( error, null );
            assert.deepEqual( result, expect );
            done();
        } );
    } );

    it( 'One line data', function ( done ) {
        data = [ 'a a a a a' ];
        var expect = [ { k: 'a', v: 5 } ];
        mr.input = function ( callback ) {
            return callback( null, data );
        };
        mr.run( function ( error, result ) {
            assert.equal( error, null );
            assert.deepEqual( result, expect );
            done();
        } );
    } );

    it( 'One data', function ( done ) {
        data = [ 'a' ];
        var expect = [ { k: 'a', v: 1 } ];
        mr.input = function ( callback ) {
            return callback( null, data );
        };
        mr.run( function ( error, result ) {
            assert.equal( error, null );
            assert.deepEqual( result, expect );
            done();
        } );
    } );

    it( 'No data', function ( done ) {
        data = [];
        var expect = [];
        mr.input = function ( callback ) {
            return callback( null, data );
        };
        mr.run( function ( error, result ) {
            assert.equal( error, null );
            assert.deepEqual( result, expect );
            done();
        } );
    } );
} );
