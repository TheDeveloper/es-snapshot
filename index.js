var Warlock = require('node-redis-warlock');
var moment = require('moment');
var async = require('async');

var redis, es, snapshotInterval, warlock, repo;

var snap = {};

var c = {
  snapshotInterval: 12*3600000,
  retention: { minimum: 10, interval: 'day', count: 30 },
  scheduleInterval: 300000
};

module.exports = function(opts) {
  es = opts.elasticsearch;
  redis = opts.redis;
  for (var p in opts) {
    c[p] = opts[p];
  }
  repo = c.repository;
  warlock = Warlock(redis);
  return snap;
};

snap.check = function check(cb) {
  warlock.lock('es-snap', c.snapshotInterval, cb);
};

snap.name = function name(){
  return moment().format('YYYY-MM-DD-HH-mm-ssZZ').replace(/\+/g, '-').toLowerCase();
};

snap.create = function create(cb) {
  var name = snap.name();
  es.snapshot.create({
    repository: repo,
    snapshot: name,
    requestTimeout: 600000
  }, function(err, d) {
    if (err) return cb(err);

    d.snapshot = name;
    return cb(err, d);
  });
};

snap.list = function list(cb) {
  es.snapshot.get({
    repository: repo,
    snapshot: '_all',
    requestTimeout: 600000
  }, function(err, d) {
    if (err) return cb(err);
    return cb(null, d.snapshots || []);
  });
};

snap.remove = function remove(snapshots, cb) {
  var d = function(s, done) {
    es.snapshot.delete({
      repository: repo,
      snapshot: s.snapshot,
      requestTimeout: 600000
    }, function(err, d) {
      if (err) return cb(err);

      cb(err, {
        snapshot: s,
        removal: d
      });
    });
  };

  async.mapLimit(snapshots, 2, d, cb);
};

snap.expired = function expired(snapshots, interval, num) {
  return snapshots.filter(function(s) {
    return moment().diff(moment(s.start_time), interval) > num;
  });
};

snap.checkRepo = function checkRepo(cb) {
  es.snapshot.getRepository({
    repository: repo
  }, cb);
};

snap.schedule = function schedule(cb) {
  var created = [], removed = [], total = 0, list = [];
  var complete = function(err) {
    return cb(err, {
      created: created,
      removed: removed,
      total: list.length,
      list: list
    });
  };

  clearTimeout(snap.timeout);
  snap.timeout = setTimeout(function () {
    async.waterfall([
      snap.check,
      function(d, done) {
        if (typeof d !== 'function') return complete();
        create(done);
      },
      function(d, done) {
        created = d;
        snap.list(done);
      },
      function(d, done) {
        list = d;
        if (d.length <= c.retention.minimum) return done(null, []);
        snap.remove(snap.expired(list, c.retention.interval, c.retention.count), done);
      }
    ], function(err, d) {
      removed = d;
      return complete(err);
    });
  }, c.scheduleInterval);
};
