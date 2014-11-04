var _     = require( 'lodash' ),
    async = require( 'async' ),
    path  = require( 'path' ),
    rootDir = path.join( '.', '..' );

var NotImplementError = require( rootDir, 'lib', 'errors' ).NotImplementError;
var GlueMapReduce = ( function () {
    return function () {
        var self = this;
        self.mode = process.argv[ 2 ];
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
            return callback( null, [] );
        };

        self._input = function ( callback ) {
            var subrun = null;
            switch ( self.mode ) {
                case 'mapper':
                case 'map':
                case 'reducer':
                case 'reduce':
                case 'red':
                    subrun = self._getStdin;
                    break;
                default:
                    subrun = self.input;
                    break;
            }
            subrun( callback );
        };
        self.run = function ( callback ) {
            self._input( function ( error, data ) {
                if ( error ) return callback( error );
                if ( !_.isArray( data ) ) return callback( 'input data is not array' );

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

        self._getStdin = function ( callback ) {
            process.stdin.resume();
            process.stdin.setEncoding( 'utf8' );
            var buffer = '';
            process.stdin.on( 'data', function ( line ) {
                buffer += line;
            } );
            process.stdin.on( 'end', function () {
                callback( null, buffer.split( '\n' ) );
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
                if ( !_.has( keyValueObject, 'k' ) || !_.has( keyValueObject, 'v' ) ) return;
                console.log( keyValueObject.k + self.mapSeparator + keyValueObject.v );
            } );
        };

        // Hadoop streaming reducer mode
        self._runHadoopReducer = function ( data, callback ) {
            var prevKey = null,
                values  = [];
            async.eachSeries( data, function ( line, next ) {
                var splited = line.split( self.reduceSeparator ),
                    key     = _.first( splited ),
                    value   = _.rest( splited ).join( self.reduceSeparator );
                if ( prevKey === key ) {
                    values.push( value );
                    return next();
                } else { // TODO
                    var myValues = values;
                    values = [ value ];
                    var myKey = prevKey;
                    prevKey = key;
                    if ( _.isNull( myKey ) ) return next();
                    self.reducer( myKey, myValues, function ( error, keyValueObjects ) {
                        if ( error ) return next( error );
                        self._outReduceResults( keyValueObjects );
                        return next( error );
                    } );
                }
            }, callback );
        };

        // Hadoop streaming reducer output
        self._outReduceResults = function ( keyValueObjects ) {
            _.each( keyValueObjects, function ( keyValueObject ) {
                if ( !_.has( keyValueObject, 'k' ) || !_.has( keyValueObject, 'v' ) ) return;
                console.log( keyValueObject.k + self.reduceSeparator + keyValueObject.v );
            } );
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
                .filter( function ( o ) { return _.isObject( o ) && _.has( o, 'k' ) && _.has( o, 'v' ) } )
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
