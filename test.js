var _     = require('underscore');
var Mocha = require('mocha');

var testConfig = {
  NODE_ENV: 'test'
};

_.each(testConfig, function(value, key){
  process.env[key] = value;
});

var mocha = new Mocha({
  timeout:  60000,
  reporter: "spec"
});

mocha.run(function(failures){
  process.on('exit', function () {
    process.exit(failures);
  });

  process.exit(0);
});

