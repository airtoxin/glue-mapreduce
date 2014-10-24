glue-mapreduce
==============

node.js mapreduce library that has high portability for hadoop framework.

##Motivation
Scalable computing is more and more important for analyse and aggregate BigData, and Map-Reduceis scalable computing algorithm for distributed platform used in Hadoop or more another service.
TODO

##Install
`npm i glue-mapreduce`

TODO: publish

##How to use

```javascript
var mr = require('glue-mapreduce');

// regist mapper
mr.mapper = function (line, callback) {
    // process line and emit key value pair
var error = null;
    return callback(error, {k: 'key', v: 'value')};
    // or emit multi pairs
    return callback(error, [
        {k: 'key1', v: 'value1'},
        {k: 'key2', v: 'value2'}
    ]);
};

// regist reducer
mr.reducer = function (key, values, callback) {
    // process values and emit it
    var error = null;
    return callback(error, values.join(','));
};

// run Map-Reduce job
mr.run(function (results) {
    // 
});
```
