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

        self.run = function ( data, callback ) {
            var subrun = null;
            switch ( self.mode ) {
                case 'mapper':
                case 'map':
                    subrun = self._runHadoopMapper;
                    break;
                case 'reducer':
                case 'red':
                    subrun = self._runHadoopReducer;
                    break;
                default:
                    subrun = self._runLocal;
                    break;
            }
            return subrun( data, callback );
        };

        // Hadoop streaming mapper mode
        self._runHadoopMapper = function ( line, callback ) {
            async.eachSeries( line, function ( datum, next ) {
                self.mapper( datum, function ( error, results ) {
                    self._logMapResults( results );
                    return next( error );
                } );
            }, function ( error ) {
                return callback( error );
            } );
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
            // body...
        };

        // optional
        self.shuffler = function () {
            // body...
        };

        Object.preventExtensions( this );
    };
}() );

exports = module.exports = GlueMapReduce;
