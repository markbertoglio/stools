//
// This module provides crypto methods. It's primary purpose is to 
// centralize crypto related functionality for security reviews.
//
var crypto = require('crypto');

module.exports = {
  generateSignature: generateSignature,
  validateSignature: validateSignature,
  generateMethodSignature: generateMethodSignature,
  encrypt: encrypt,
  decrypt: decrypt
};

function encrypt(object, key, algorithm, inputEncoding, outputEncoding) {
  algorithm = !algorithm ? 'des' : algorithm;
  inputEncoding = !inputEncoding ? 'utf8' : inputEncoding;
  outputEncoding = !outputEncoding ? 'base64' : outputEncoding;
  var cipher = crypto.createCipher(algorithm, key);
  var encryptedData = cipher.update(object, inputEncoding, outputEncoding);
  encryptedData += cipher.final(outputEncoding); 
  return encryptedData;
}

function decrypt(encryptedData, key, algorithm, inputEncoding, outputEncoding) {
  try {
    algorithm = !algorithm ? 'des' : algorithm;
    inputEncoding = !inputEncoding ? 'base64' : inputEncoding;
    outputEncoding = !outputEncoding ? 'utf8' : outputEncoding;
    var decipher = crypto.createDecipher(algorithm, key);
    var object = decipher.update(encryptedData, inputEncoding, outputEncoding);
    object += decipher.final(outputEncoding);
    return object;
  } catch (e) {
    return null;
  }
}

function generateSignature() {
  var args = Array.prototype.slice.call(arguments);
  return crypto.createHash('md5').update(args.join()).digest('hex');
}

function validateSignature() {
  var args = Array.prototype.slice.call(arguments);
  var signature = args.shift();
  return signature === crypto.createHash('md5').update(args.join()).digest('hex');
}

function generateMethodSignature(secret, args) {
  var hashStr = getHashString();
  return crypto.createHash('sha256').update(hashStr).digest('hex');

  function getHashString() {
    var sortedArgs = sortObject(args);
    delete sortedArgs.requestSignature;
    var str = '';
    Object.keys(sortedArgs).forEach(function (key) {
      if (typeof args[key] == 'string') str = str + args[key];
    });
    return [str, secret].join('').toLowerCase();
  }

  function sortObject(o) {
    var sorted = {},
    key, a = [];

    for (key in o) {
      if (o.hasOwnProperty(key)) {
        a.push(key);
      }
    }

    a.sort();

    for (key = 0; key < a.length; key++) {
      sorted[a[key]] = o[a[key]];
    }
    return sorted;
  }
}
