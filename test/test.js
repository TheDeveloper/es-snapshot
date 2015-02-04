var snap = require('../')({
  redis: require('redis').createClient(),
  elasticsearch: require('elasticsearch').Client({log:'trace'}),
  repository: 's3-backups'
});
var assert = require('assert');
var log = process.env.LOG && function() { console.log(JSON.stringify(arguments, null, 2)); } || function(){};

describe('es-snapshot', function() {
  it('name', function() {
    log(snap.name());
  });

  it('check', function(done) {
    snap.checkRepo(function(err, d) {
      log(err, d);
      assert.equal(err, null);
      assert.equal(typeof d, 'object');
      return done(err);
    });
  });

  it('list', function(done) {
    snap.list(function(err, d) {
      log(err, d);
      assert.equal(err, null);
      assert(Array.isArray(d));
      var e = snap.expired(d, 'day', 1);
      assert(e.length);
      return done(err);
    });
  });

  it('create', function(done) {
    snap.create(function(err, d) {
      log(err, d);
      assert.equal(err, null);
      assert(d.accepted);
      snap.remove([{ snapshot: d.snapshot }]);
      done(err);
    });
  });
});
