//
// Simple library to support method chaining.
// Only supports options. For Usage, see tchain.js
//

var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');
module.exports = chain;

function chain(obj, defaultOptions) {
  var event = new EventEmitter();

  obj = obj || {};
  obj.set = set;
  obj.unset = unset;
  obj.on = on;
  obj.append = append;
  obj.onError = onError;

  var options = {set: defaultOptions || {}, unset: {}, append: {}};
  var promise = _.extend(event, {
    getOptions: getOptions,
    getOption: getOption,
    addFlag: addFlag,
    addFlags: addFlags,
    addExplicitSetters: addExplicitSetters,
    fire: fire
  });
  return promise;

  function getOptions() { 
    return options; 
  }
  
  function getOption(name) {
    var ret = {};
    if (options.set.hasOwnProperty(name)) {
      ret.set = options.set[name];
    }
    if (options.append.hasOwnProperty(name)) {
      ret.append = options.append[name];
    }
    if (options.unset.hasOwnProperty(name)) {
      ret.unset = true;
    }
    return ret;
  }
  
  function addFlag(name, setter, resetter) {
    if (!setter && !resetter) {
      setter = "set" + name[0].toUpperCase() + name.slice(1);
      resetter = "reset" + name[0].toUpperCase() + name.slice(1);
    }
    if (setter) obj[setter] = setFlag;
    if (resetter) obj[resetter] = resetFlag;
    return promise;
    
    function setFlag(value) {
      return obj.set(name, (arguments.length === 0) || value);
    }
    function resetFlag(value) {
      return obj.set(name, (arguments.length !== 0) && !value);
    }
  }

  function addFlags(names) {
    for (var kk = 0; kk < names.length; kk ++) {
      promise.addFlag(names[kk]);
    }
    return promise;
  }

  function addExplicitSetters() {
    var names = Array.prototype.slice.call(arguments);
    for (var kk = 0; kk < names.length; kk ++) addExplicitSetter(names[kk]);
    function addExplicitSetter(name) {
      obj[name] = function(value) {
        options.set[name] = value;
        return obj;
      };
    }
    return promise;
  }

  // utility function to fire.
  function fire() {
    var args = Array.prototype.slice.call(arguments);
    if (args.length >= 1 && !!args[0]) {
      args[0] = new Error(args[0]);
      args.unshift('error');
    } else {
      args.unshift('done');
    }
    event.emit.apply(event, args);
    // we need to remove listeners after firing to avoid leaks
    event.removeAllListeners();
  }

  // these functions are hoisted.
  function set(name, value) {
    if (typeof name == 'object' && !value) {
      // add a bunch of keys and values
      _.extend(options.set, name);
    } else {
      options.set[name] = value;
    }
    return obj;
  }

  function append(name, value) {
    if (typeof name == 'object' && !value) {
      // add a bunch of keys and values
      _.extend(options.append, name);
    } else {
      options.append[name] = value;
    }
    return obj;
  }

  function unset(name) {
    options.unset[name] = true;
    return obj;
  }

  function on(fn) {
    event.on('done', fn);
    return obj;
  }

  function onError(fn) {
    event.on('error', fn);
    return obj;
  }
}
