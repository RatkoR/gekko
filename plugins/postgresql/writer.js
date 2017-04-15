var _ = require('lodash');
var config = require('../../core/util.js').getConfig();

var handle = require('./handle');
var postgresUtil = require('./util');

var Store = function(done, pluginMeta) {
  _.bindAll(this);
  this.done = done;

  this.db = handle;
  this.upsertTables();

  this.cache = [];
}

Store.prototype.upsertTables = function() {
  var version = config.candleWriter.version;

  var createQueries = [
    prepareTableSql(version)
  ];

  var next = _.after(_.size(createQueries), this.done);

  _.each(createQueries, function(q) {
    this.db.query(q,next);
  }, this);
}

Store.prototype.writeCandles = function() {
  if(_.isEmpty(this.cache)){
    return;
  }

  var version = config.candleWriter.version;

  var stmt = prepareCandleSql(version);

  _.each(this.cache, candle => {
    this.db.query(stmt, prepareCandleData(candle, version));
  });

  this.cache = [];
}

var processCandle = function(candle, done) {

  // because we might get a lot of candles
  // in the same tick, we rather batch them
  // up and insert them at once at next tick.
  this.cache.push(candle);
  _.defer(this.writeCandles);
  done();
}

if(config.candleWriter.enabled){
  Store.prototype.processCandle = processCandle;
}

var prepareCandleData = function(candle, version) {
  var data = [
    candle.start.unix(),
    candle.open,
    candle.high,
    candle.low,
    candle.close,
    candle.vwp,
    candle.volume,
    candle.trades
  ];

  if(version === 2) {
    data = data.concat([
      candle.buyVolume,
      candle.buyTrades,
      candle.lag,
      JSON.stringify(candle.raw)
    ]);
  }

  return data;
}

var prepareCandleSql = function(version) {
  switch(version) {
    case 2:
      var sql = `
        INSERT INTO ${postgresUtil.table('candles')}
        (start, open, high,low, close, vwp, volume, trades, buy_volume, buy_trades, lag, raw)
        values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::JSON) ON CONFLICT DO NOTHING;`;
      break;
    default:
      var sql = `
        INSERT INTO ${postgresUtil.table('candles')}
        (start, open, high,low, close, vwp, volume, trades)
        values($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING;`;
  }

  return sql;
}

var prepareTableSql = function(version) {
  var fields = `
      id BIGSERIAL PRIMARY KEY,
      start integer UNIQUE,
      open double precision NOT NULL,
      high double precision NOT NULL,
      low double precision NOT NULL,
      close double precision NOT NULL,
      vwp double precision NOT NULL,
      volume double precision NOT NULL,
      trades INTEGER NOT NULL
  `;

  if(version === 2) {
    fields += `,
      buy_volume double precision NOT NULL,
      buy_trades INTEGER NOT NULL,
      lag INTEGER NOT NULL,
      raw jsonb
    `;
  }

  return `
    CREATE TABLE IF NOT EXISTS
      ${postgresUtil.table('candles')} (
      ${fields}
    );`
}

module.exports = Store;
