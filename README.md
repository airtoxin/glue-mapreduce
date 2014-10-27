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
###Script
```javascript
var mr = new ( require('glue-mapreduce') )();

var data = fs.readFileSync('somefile.txt').toString().split('\n');

// regist mapper
mr.mapper = function (mapLine, callback) {
    // mapper called per iteration of input data
    var error = null;
    var split = mapLine.split(' ');
    var key = split[0],
        val = split[1];

    return callback(error, [{k: key, v: val}]};
    // callback can be return multi key-value pairs
};

// regist reducer
mr.reducer = function (key, values, callback) {
    // reducer called per iteration of keys

    return callback(error, [{k: key, v: values.length}]);
    // callback can be return multi key-value pairs
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
###run options
glue-mapreduce make a decision about whether to run with local Map-Reduce or Hadoop streaming mapper or reducer by command-line argument.

To run __local Map-Reduce__, `node somemapreduce.js local`

To run __Hadoop Streaming Mapper__, `hadop jar hadoop-streaming.jar -mapper 'somemapreduce.js mapper' ...`

To run __Hadoop Streaming Reducer__, `hadoop jar hadoop-streaming.jar -reducer 'somemapreduce.js reducer' ...`

__Important__: To quote command need to assign argument.

These behavior also can control by `mr.mode` variable. e.g. `mr.mode = 'local'` on script, it runs local Map-Reduce default (with no argument).
