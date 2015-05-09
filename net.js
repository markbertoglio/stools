var http = require('http');

module.exports = {
  httpPost: httpPost
};

function httpPost(host, port, path, body, session, done) {
  if (typeof session == 'function') {
    done = session;
    session = null;
  }
  var bodyStr = JSON.stringify(body);

  var headers = {
    'Content-Type': 'application/json',
    'Content-Length': bodyStr.length
  };

  if (session) headers['session-token'] = session; 

  var options = {
    host: host,
    port: port,
    path: path,
    method: 'POST',
    headers: headers
  };

  // Setup the request.  The options parameter is
  // the object we defined above.
  var req = http.request(options, function(res) {
    res.setEncoding('utf-8');

    var responseString = '';

    res.on('data', function(data) {
      responseString += data;
    });

    res.on('end', function() {
      var resultObject = JSON.parse(responseString);
      done(null, resultObject);
    });
  });

  req.on('error', function(e) {
    done(e);
  });

  req.write(bodyStr);
  req.end();
}