var Warlock = require('node-redis-warlock');
var moment = require('moment');
var async = require('async');

module.exports = function(opts) {
  var redis, es, snapshotInterval, warlock, repo;

  var snap = {};

  var c = {
    snapshotInterval: 12*3600000,
    retention: {
      // Time granularity to use for retention period
      interval: 'day',
      // Number of interval to use for retention period
      num_intervals: 30,
      // Minimum number of snapshots to retain regardless of retention period
      minimum: 10
    },
    lockKey: 'es-snap'
  };

  es = opts.elasticsearch;
  redis = opts.redis;
  for (var p in opts) {
    c[p] = opts[p];
  }
  repo = c.repository;
  warlock = Warlock(redis);

  snap.checkLock = function checkLock(cb) {
    warlock.lock(c.lockKey, c.snapshotInterval, cb);
  };

  snap.name = function name(){
    return moment().format('YYYY-MM-DD-HH-mm-ssZZ').replace(/\+/g, '-').toLowerCase();
  };

  snap.create = function create(cb) {
    var name = snap.name();
    es.snapshot.create({
      repository: repo,
      snapshot: name,
      requestTimeout: 60000*30,
      waitForCompletion: true
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
      requestTimeout: 60000*30
    }, function(err, d) {
      if (err) return cb(err);
      return cb(null, d.snapshots || []);
    });
  };

  snap.remove = function remove(snapshots, cb) {
    var del = function(s, done) {
      es.snapshot.delete({
        repository: repo,
        snapshot: s.snapshot,
        requestTimeout: 60000*30
      }, function(err, d) {
        if (err) return done(err);

        done(err, {
          snapshot: s,
          removal: d
        });
      });
    };

    async.mapLimit(snapshots, 1, del, cb);
  };

  snap.expired = function expired(snapshots, interval, num) {
    return snapshots.filter(function(s) {
      return moment().diff(moment(s.start_time), interval) > num;
    });
  };

  snap.checkRepo = function checkRepo(cb) {
    es.snapshot.getRepository({
      repository: repo
    }, function(err, d) {
      cb(err, d);
    });
  };

  snap.run = function run(cb) {
    var created, removed = [], total = 0, list = [], locked;
    var complete = function(err) {
      return cb(err, {
        created: created,
        removed: removed,
        total: list.length,
        list: list,
        locked: locked
      });
    };

    async.waterfall([
      function(done) {
        snap.checkLock(done);
      },
      function(d, done) {
        locked = typeof d !== 'function';
        if (locked) return complete();
        snap.checkRepo(done);
      },
      function (d, done) {
        snap.create(done);
      },
      function(d, done) {
        created = d;
        snap.list(done);
      },
      function(d, done) {
        list = d;
        if (d.length <= c.retention.minimum) return done(null, []);
        var exp = snap.expired(list, c.retention.interval, c.retention.num_intervals);
        snap.remove(exp, done);
      }
    ], function(err, d) {
      if (err) return complete(err);
      removed = d;
      return complete();
    });
  };

  return snap;
};
