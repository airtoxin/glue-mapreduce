var _     = require( 'lodash' ),
    async = require( 'async' ),
    path  = require( 'path' ),
    rootDir = path.join( '.', '..' );

var NotImplementError = require( rootDir, 'lib', 'errors' ).NotImplementError;
var GlueMapReduce = ( function () {
    return function () {
        var self = this;
        self.mode = null;
        self.mapSeparator    = '\t';
        self.reduceSeparator = '\t';

        /* Implement */
        self.mapper  = function ( line, callback ) {
            throw new NotImplementError();
        };

        /* Implement */
        self.reducer = function ( key, values, callback ) {
            throw new NotImplementError();
        };

        /* Implement */
        self.input = function ( callback ) {
            throw new NotImplementError();
        };

        self.run = function ( callback ) {
            self.input( function ( error, data ) {
                if ( error ) return callback( error );
                if ( _.isUndefined( data.length ) ) return callback( 'input data can not iterable' );

                var subrun = null;
                switch ( self.mode ) {
                    case 'mapper':
                    case 'map':
                        subrun = self._runHadoopMapper;
                        break;
                    case 'reducer':
                    case 'reduce':
                    case 'red':
                        subrun = self._runHadoopReducer;
                        break;
                    default:
                        subrun = self._runLocal;
                        break;
                }
                return subrun( data, callback );
            } );
        };

        // Hadoop streaming mapper mode
        self._runHadoopMapper = function ( data, callback ) {
            // async.eachSeries( line, function ( datum, next ) {
            //     self.mapper( datum, function ( error, results ) {
            //         self._logMapResults( results );
            //         return next( error );
            //     } );
            // }, function ( error ) {
            //     return callback( error );
            // } );
            return callback( error );
        };

        // Hadoop streaming mapper output
        self._logMapResults = function ( results ) {
            _.each( results, function ( result ) {
                var line = result.k + self.mapSeparator + result.v;
                console.log( line );
            } );
        };

        // Hadoop streaming reducer mode
        self._runHadoopReducer = function ( data, callback ) {
            // body...
        };

        // local Map-Reduce mode
        self._runLocal = function ( data, callback ) {
            async.waterfall( [
                function ( next ) {
                    // map
                    self._applyLocalMapper( data, next );
                },
                function ( next, keyValues ) {
                    // shuffle
                    self.shuffler( keyValues, next );
                },
                function ( next ) {
                    // reduce
                    next();
                }
            ], function ( error ) {
                return callback( error, result );
            } );
        };

        self._applyLocalMapper = function ( data, callback ) {
            async.eachSeries( data, function ( line, next ) {
                self.mapper( line, mapperCallback );
            }, function ( error ) {
                return callback( error, results );
            } );
        };

        // optional
        self.shuffler = function ( keyValues, callback ) {
            keyValues = _.chain( keyValues )
                .filter( _.isObject )
                .groupBy( 'k' )
                .map( function ( values, key ) {
                    var datum = {};
                    datum[ key ] = _.map( values, function ( o ) { return o.v; } );
                    return datum;
                } )
                .value();
            return callback( null, keyValues );
        };

        Object.preventExtensions( this );
    };
}() );

exports = module.exports = GlueMapReduce;
