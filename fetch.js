//
// Simple module to manage fetching data in parallel
//

var _ = require('underscore');
module.exports = fetch;

var global = {};

function fetch() {
  var state = {
    counter: 0,
    events: [],
    values: {},
    historicEvents: []
  };

  state.obj = {
    throttle: throttle.bind(null, state),
    fork: fork.bind(null, state),
    require: fetchRequire.bind(null, state),
    provide: fetchProvide.bind(null, state),
    error: fetchProvide.bind(null, state, 'error'),
    onError: fetchRequire.bind(null, state, 'error'),
    cancel: fetchCancel.bind(null, state),
    requireAndProvide: fetchRequireAndProvide.bind(null, state)
  };
  return state.obj;
}

function throttle(state, max, queueName) {
  var throttleState = {};
  if (queueName) {
    global[queueName] = global[queueName] || {};
    throttleState = global[queueName];
  }
  throttleState.max = max;
  throttleState.count = throttleState.count || 0;
  throttleState.pending = throttleState.pending || [];

  return _.extend({}, state.obj, {fork: throttledFork});

  function throttledFork(requires, value) {
    if (typeof(requires) == 'function') {
      value = requires;
      requires = [];
    }

    // pending is what will get called when the throttle is opened and the result is available
    var pending = null;
    var ret = fork(state, function (done) { pending = done; });

    // wait for throttle to open and acquire it then
    fork(state, requires, function () {
      var wait = fork(state, [], waitForToken);
      fork(state, wait, function () {

        // when throttle is open, reissue the query to get actual result
        var result = fork(state, requires, value);
        fork(state, result, function (_result) { 

          // open the throttle for next caller
          releaseToken();

          // once actual ret value is available, complete original request
          pending(null, result); 
        });
      });
    });
    return ret;
  }

  function waitForToken(done) {
    throttleState.count ++;
    if (throttleState.count <= throttleState.max) return done();
    throttleState.pending.push(done);
  }

  function releaseToken() {
    throttleState.count --;
    if (throttleState.pending.length) return setTimeout(throttleState.pending.shift());
  }
}

function fetchCancel(state) {
  state.canceled = true;
  return state.obj;
}

function fetchRequireAndProvide(state, requires, provides, value) {
  if (!Array.isArray(requires)) requires = [requires];
  state.events.push({
    requires: requires, 
    provides: provides, 
    value: value, 
  });
  runReady(state);
  return state.obj;
}

function fork(state, requires, value) {
  // similar to requires but it creates a fake internal id instead
  var provides = 'fork:' + (++state.counter);
  if (typeof requires === 'function') {
    value = requires; 
    requires = []; 
  }

  if (!Array.isArray(requires)) requires = [requires];
  requires = _.map(requires, (function (req) {
    if (typeof(req) == 'string' && req.indexOf('fork:') === 0) return req;
    var ret = 'fork:' + (++state.counter);
    state.events.push({requires: [], provides: ret, value: req, stack: []});
    return ret;
  }));
  
  fetchRequireAndProvide(state, requires, provides, value);
  return provides;
}

function runReady(state) {
  var ran;
  do {
    ran = false;
    for (var kk = 0; kk < state.events.length; kk ++) {
      var fn = checkReady(state, state.events[kk]);
      if (fn === false) continue;

      var event = state.events.splice(kk, 1);
      ran = true;
      fn();
      break;
    }
  } while (ran);
}

function checkReady(state, requirement) {
  if (state.canceled && requirement.requires.toString() != ['error'].toString()) {
    // canceled state will still raise error events but nothing else
    return false;
  }

  var args = [];
  var ready = true;
  requirement.requires.forEach(function (event) {
    if (state.values.hasOwnProperty(event)) {
      args.push(state.values[event]);
    } else {
      ready = false;
    }
  });

  if (!ready) return false;
  return function () {
    callRequirement(state, requirement, args);
  }; 
}

function callPromise(state, requirement, args) {
  // we expect a response from the promise, so register for promise callback
  requirement.value.onError(function (err) {
    return fetchRequireAndProvide(state, [], 'error', err);
  });
  requirement.value.on(function (err, result) {
    return fetchRequireAndProvide(state, [], requirement.provides, result);
  });
  return requirement.value.apply(null, args);
}

function firePromise(state, requirement, args) {
  if (requirement.requires.indexOf('error') != -1) {
    return requirement.value.fire.apply(null, args);
  }
  args.unshift(null);
  requirement.value.fire.apply(null, args);
}

function callRequirement(state, requirement, args) {
  var value = requirement.value;

  // if it is an actual promise, fire it.
  if (value && typeof value.fire == 'function') {
    return firePromise(state, requirement, args);
  }

  // if it is a promise-chain, check if it provides something.
  if (typeof value == 'function' && typeof value.on == 'function') {
    return callPromise(state, requirement, args);
  }

  // straight result
  if (typeof value != 'function') {
    if (requirement.provides) {
      state.values[requirement.provides] = value;
      if (requirement.provides == 'error') state.canceled = true;
    }
    return;
  }

  // if it is a node.js style callback (err, result) 
  if (value.length == args.length + 1) {
    args.push(function onDone(err, result) {
      if (err) return fetchRequireAndProvide(state, [], 'error', err);
      return fetchRequireAndProvide(state, [], requirement.provides, result);
    });
    return value.apply(null, args);
  }
  
  // it is a simple sync callback. call and recurse in case it
  // returns a promise or a callback
  return fetchRequireAndProvide(
    state, [], requirement.provides, 
    value.apply(null, args)
  );
}

function fetchRequire(state, event, value) {
  return fetchRequireAndProvide(state, event, null, value);
}

function fetchProvide(state, event, value) {
  return fetchRequireAndProvide(state, [], event, value);
}
