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
mr.mapper = function (line) {
    // emit key value paired object
    return {k: 'key', v: 'value'};
    // emit multi key value paired objects allowed
    return [
        {k: 'key1', v: 'value1'},
        {k: 'key2', v: 'value2'}
    ];
};

// regist reducer
mr.reducer = function (key, values) {
    // TODO
};
```
