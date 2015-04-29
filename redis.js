var url = require('url');
var chain = require('./chain');
var fetch = require('./fetch');
var duration = require('length');
var _ = require('underscore');
var log = require('npmlog');
var redis = require('redis');

var LP = 'redis';

module.exports = {
  getRedisClient: getRedisClient,
  getPublisher: getPublisher,
  getSubscriber: getSubscriber,
  getKeys: getKeys,
  setObj: setObj,
  getObj: getObj,
  delObj: delObj,
  setObjField: setObjField,
  getObjField: getObjField,
  enqueue: enqueue,
  readQueue: readQueue,
  trimQueue: trimQueue
};

function createClient(done) {
  var host = process.env.REDIS_HOST;
  var port = parseInt(process.env.REDIS_PORT);
  var pw = process.env.REDIS_PW;
  var client = redis.createClient(port, host);
  log.info(LP, "connection is %s:%d %s.", host, port, pw);
  if (pw) {
    client.auth(pw, authCallback);
  }        
  client.on('ready', readyCallback);
  client.on('error', errorCallback);
  return client;

  function authCallback() {
  }

  function readyCallback() {
    done(null, client);
  }

  function errorCallback(err) {
    done(err);
  }
}

var redisClient;
function getRedisClient(done) {
  if (redisClient) return done(null, redisClient);
  redisClient = createClient(function (err, client) {
    if (err) {
      redisClient = null;
      return done(err);
    }
    client.on('error', function(e) { redisClient = null; });
    return done(null, redisClient = client);
  });
}

var subscriber;
function getSubscriber(done) {
  if (subscriber) return done(null, subscriber);
  createClient(function (err, client) {
    if (err) return done(err);
    client.on('error', function () { subscriber = null; });
    return done(null, subscriber = client);
  });
}

function getPublisher(done) {
  // publish can share connection with api
  return getRedisClient(done);
}

function getKeys(keyPrefix) {
  var promise = chain(doGetKeys, {});
  return doGetKeys;

  function doGetKeys() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, _getKeys);
    fetcher.require(result, function(result) { 
      return promise.fire(null, result);
    });

    function _getKeys(redisClient, done) {
      redisClient.keys(keyPrefix, done);
    }
  }
}

function setObj(objId, key, value, TTL) {
  var promise = chain(doSetObj, {});
  return doSetObj;

  function doSetObj() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, setValue);
    var ttlSet = fetcher.fork([redisClient, result], setExpire);
    fetcher.require([ttlSet, result], function(ttlSet, result) { 
      return promise.fire(null, result);
    });
    
    function setValue(redisClient, done) {
      redisClient.set(getKey(objId, key), JSON.stringify(value), done);
    }

    function setExpire(redisClient, notUsed, done) {
      if (!TTL) return done();
      redisClient.expire(getKey(objId, key), duration(TTL, 's'), done);
    }
  }
}

function getObj(objId, key) {
  log.info(LP, "get %s", getKey(objId, key));
  var promise = chain(doGetObj, {});
  return doGetObj;

  function doGetObj() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, getValue);
    fetcher.require(result, function(result) { 
      var value; // If no result then return Undefined
      if (result) value = JSON.parse(result);
      return promise.fire(null, value);
    });
    
    function getValue(redisClient, done) {
      redisClient.get(getKey(objId, key), done);
    }
  }
}

function delObj(objId, key) {
  var promise = chain(doDelObj, {});
  return doDelObj;

  function doDelObj() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, delValue);
    fetcher.require(result, function(result) { 
      return promise.fire(null, result);
    });
    
    function delValue(redisClient, done) {
      redisClient.del(getKey(objId, key), done);
    }
  }
}

function setObjField(objId, key, field, value, TTL) {
  var promise = chain(doSetObjField, {});
  return doSetObjField;

  function doSetObjField() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, setValue);
    var ttlSet = fetcher.fork([redisClient, result], setExpire);
    fetcher.require([ttlSet, result], function(ttlSet, result) { 
      return promise.fire(null, result);
    });
    
    function setValue(redisClient, done) {
      redisClient.hset(getKey(objId, key), field, JSON.stringify(value), done);
    }

    function setExpire(redisClient, notUsed, done) {
      if (!TTL) return done();
      redisClient.expire(getKey(objId, key), duration(TTL, 's'), done);
    }
  }
}

function getObjField(objId, key, optionalField) {
  var promise = chain(doGetObj, {});
  return doGetObj;

  function doGetObj() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, getValue);
    fetcher.require(result, function(result) { 
      var value; // If no result then return Undefined
      if (result) {
        if (_.isObject(result)) {
          value = {};
          Object.keys(result).forEach(function(key) {
            value[key] = JSON.parse(result[key]);
          });
        } else {
          value = JSON.parse(result);
        }
      }
      return promise.fire(null, value);
    });
    
    function getValue(redisClient, done) {
      if (optionalField) return redisClient.hget(getKey(objId, key), optionalField, done);
      redisClient.hgetall(getKey(objId, key), done);
    }
  }
}

function getKey(objId, key) {
  return [objId, key].join(':');
}

function escapeRegExp(value) {
  return value.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, "\\$&");
}

function getPathWithParams(path, params) {
  var keys = _.map(Object.keys(params), escapeRegExp);
  if (keys.length) {
    path = path.replace(new RegExp(
      '(' + keys.join('|') + ')', 'g' ), function replaceit(s, key) {
        return params[key] || s;
      });
  }
  return path;
}

function trimQueue(path, params, size) {
  if (arguments.length == 2) {
    value = params;
    params = {};
  }
  path = getPathWithParams(path, params);
  var promise = chain(doTrim, {});
  return doTrim;

  function doTrim() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, setSize);
    fetcher.require([result], function () { promise.fire(null); });
  }

  function setSize(redisClient, done) {
    redisClient.ltrim(path, 0, size || 1000, done);
  }
}

function enqueue(path, params, value) {
  if (arguments.length == 2) {
    value = params;
    params = {};
  }
  path = getPathWithParams(path, params);
  var promise = chain(doEnqueue, {});
  return doEnqueue;

  function doEnqueue() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, setValue);
    var ttlSet = fetcher.fork([redisClient, result], setExpire);
    fetcher.require([ttlSet, result], function(ttlSet, result) { 
      return promise.fire(null, result);
    });
    
    function setValue(redisClient, done) {
      // LPUSH path value; LRANGE path 0 limit-1
      redisClient.multi()
        .lpush(path, JSON.stringify(value))
        .ltrim(path, 0, (promise.getOption('limit').set || params.limit || 1000) - 1)
        .exec(function (err, replies) {
          return done(err);
        });
    }

    function setExpire(redisClient, notUsed, done) {
      var ttl = promise.getOption('ttl').set || params.ttl;
      if (!ttl) return done();
      redisClient.expire(path, duration(ttl, 's'), done);
    }
  }
}

function readQueue(path, params) {
  path = getPathWithParams(path, params || {});
  var promise = chain(doReadQueue, {});
  return doReadQueue;

  function doReadQueue() {
    var fetcher = fetch().onError(promise);
    var redisClient = fetcher.fork(getRedisClient);
    var result = fetcher.fork(redisClient, getValue);
    fetcher.fork(result, function (result) { promise.fire(null, result); });

    function getValue(redisClient, done) {
      var start = params.start || 0;
      var end = params.end || -1;
      redisClient.lrange(path, start, end, function (err, result) {
        if (err) return done(err);
        if (Array.isArray(result)) {
          return done(null, _.map(result, JSON.parse.bind(JSON)));
        }
      });
    }
  }
}
