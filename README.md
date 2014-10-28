glue-mapreduce
==============

node.js mapreduce library that has high portability for hadoop framework.

##Motivation
Scalable computing is more and more important for analyse and aggregate BigData, and Map-Reduce is scalable computing algorithm for distributed platform used in Hadoop or more another service.

It is important for think about scalable programing to stable running for increasing data day by day. But case of "Map-Reduce is exaggerated for current data size (MB~GB order), but ensure scalability." are very often request.

I solve these problems to run local Map-Reduce aggregation that have portablility to Hadoop platforms.

##Install
`npm i glue-mapreduce`

TODO: publish

##How to use

```javascript
var mr = new ( require('glue-mapreduce') )();

var data = fs.readFileSync('somefile.txt').toString().split('\n');

// regist mapper
mr.mapper = function (mapLine, callback) {
    // process line and emit key value pair
    var error = null;
    var split = mapLine.split(' ');
    var key = split[0],
        val = split[1];

    return callback(error, {k: key, v: val)};
    // or emit multi pairs
    return callback(error, [
        {k: 'key1', v: 'value1'},
        {k: 'key2', v: 'value2'}
    ]);
};

// regist reducer
mr.reducer = function (key, values, callback) {
    // process values and emit it with key
    var emitData = [{k: key, v: values.length}];
    return callback(error, emitData);
};

// run Map-Reduce job
mr.run(data, function (results) {
    /*
    results is array of key value pair
    [{
        k: 'key',
        v: 100 (reduced values)
    }, ...]
    */
});
```
