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
            async.eachSeries( data, function ( line, next ) {
                self.mapper( line, function ( error, keyValueObjects ) {
                    if ( error ) return next( error );
                    self._outMapResults( keyValueObjects );
                    return next( error );
                } );
            }, callback );
        };

        // Hadoop streaming mapper output
        self._outMapResults = function ( keyValueObjects ) {
            _.each( keyValueObjects, function ( keyValueObject ) {
                console.log( keyValueObject.k + self.mapSeparator + keyValueObject.v );
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
                function ( keyValueObjects, next ) {
                    // shuffle
                    self.shuffler( keyValueObjects, next );
                },
                function ( keyValuesObjects, next ) {
                    // reduce
                    self._applyLocalReducer( keyValuesObjects, next );
                }
            ], function ( error, results ) {
                return callback( error, results );
            } );
        };

        self._applyLocalMapper = function ( data, callback ) {
            async.mapSeries( data, self.mapper, function ( error, results ) {
                return callback( error, _.flatten( results ) );
            } );
        };

        self._applyLocalReducer = function ( keyValuesObjects, callback ) {
            async.mapSeries( keyValuesObjects, function ( keyValuesObject, next ) {
                self.reducer( keyValuesObject.k, keyValuesObject.v, function ( error, keyValueObjects ) {
                    next( error, keyValueObjects );
                } );
            }, function ( error, results ) {
                return callback( error, _.flatten( results ) );
            } );
        };

        // optional
        self.shuffler = function ( keyValueObjects, callback ) {
            keyValueObjects = _.chain( keyValueObjects )
                .filter( _.isObject )
                .groupBy( 'k' )
                .map( function ( values, key ) {
                    var datum = {};
                    datum.k = key;
                    datum.v = _.map( values, function ( o ) { return o.v; } );
                    return datum;
                } )
                .value();
            return callback( null, keyValueObjects );
        };

        Object.preventExtensions( this );
    };
}() );

exports = module.exports = GlueMapReduce;
