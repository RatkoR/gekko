// The CandleCreator creates one minute candles based on trade batches. Note
// that it also adds empty candles to fill gaps with no volume.
//
// Expects trade batches to be written like:
//
// {
//   amount: x,
//   start: (moment),
//   end: (moment),
//   first: (trade),
//   last: (trade),
//   timespan: x,
//   all: [
//      // batch of new trades with
//      // moments instead of timestamps
//   ]
// }
//
// Emits 'new candles' event.
//
// There are two candle versions currently. Which
// one is sent depends on the config.candleWriter.version variable.
//
// Version 1 is the default.
// Version 2 has additional fields, see below which ones.
//
// Candle version 2 (and above) has also a 'version' field in candle
// data for quick checking.
//
// [
//     {
//       start: (moment),
//       end: (moment),
//       high: (float),
//       open: (float),
//       low: (float),
//       close: (float)
//       volume: (float)
//       vwp: (float) // volume weighted price,
//       trades: (integer), // nb of all trades
//       // ---- v2 fields -----
//       version: 2,
//       buyVolume: (float), // volume of buy trades
//       buyTrades: (integer), // nb of buy trades,
//       lag: (integer), // exchange lag,
//       raw: (array) // array of all trades
//    },
//    {
//       start: (moment), // + 1
//       end: (moment),
//       high: (float),
//       open: (float),
//      low: (float),
//      close: (float)
//       volume: (float)
//       vwp: (float) // volume weighted price
//       trades: (integer), // nb of all trades
//       // ---- v2 fields -----
//       version: 2,
//       buyVolume: (float), // volume of buy trades
//       buyTrades: (integer), // nb of buy trades,
//       lag: (integer), // exchange lag,
//       raw: (array) // array of all trades
//    }
//    // etc.
// ]
//

var _ = require('lodash');
var moment = require('moment');
var config = require('../../core/util.js').getConfig();

var util = require(__dirname + '/../util');

var CandleCreator = function() {
  _.bindAll(this);

  // TODO: remove fixed date
  this.threshold = moment("1970-01-01", "YYYY-MM-DD");

  // This also holds the leftover between fetches
  this.buckets = {};

  // exchange lag
  this.lag = 0;
}

util.makeEventEmitter(CandleCreator);

CandleCreator.prototype.write = function(batch) {
  var trades = batch.data;

  if(_.isEmpty(trades))
    return;

  this.lag = batch.lag;

  var candleVersion = config.candleWriter.version;

  trades = this.filter(trades);
  this.fillBuckets(trades);
  var candles = this.calculateCandles(candleVersion);

  candles = this.addEmptyCandles(candles, candleVersion);

  // the last candle is not complete
  this.threshold = candles.pop().start;

  this.emit('candles', candles);
}

CandleCreator.prototype.filter = function(trades) {
  // make sure we only include trades more recent
  // than the previous emitted candle
  return _.filter(trades, function(trade) {
    return trade.date > this.threshold;
  }, this);
}

// put each trade in a per minute bucket
CandleCreator.prototype.fillBuckets = function(trades) {
  _.each(trades, function(trade) {
    var minute = trade.date.format('YYYY-MM-DD HH:mm');

    if(!(minute in this.buckets))
      this.buckets[minute] = [];

    this.buckets[minute].push(trade);
  }, this);

  this.lastTrade = _.last(trades);
}

// convert each bucket into a candle
CandleCreator.prototype.calculateCandles = function(candleVersion) {
  var minutes = _.size(this.buckets);

  // catch error from high volume getTrades
  if (this.lastTrade !== undefined)
    // create a string referencing to minute this trade happened in
    var lastMinute = this.lastTrade.date.format('YYYY-MM-DD HH:mm');

  var candles = _.map(this.buckets, function(bucket, name) {
    var candle = this.calculateCandle(bucket, candleVersion);

    // clean all buckets, except the last one:
    // this candle is not complete
    if(name !== lastMinute)
      delete this.buckets[name];

    return candle;
  }, this);

  return candles;
}

CandleCreator.prototype.calculateCandle = function(trades, candleVersion) {
  var first = _.first(trades);

  var f = parseFloat;

  var candle = {
    start: first.date.clone().startOf('minute'),
    open: f(first.price),
    high: f(first.price),
    low: f(first.price),
    close: f(_.last(trades).price),
    vwp: 0,
    volume: 0,
    trades: _.size(trades)
  };

  if(candleVersion === 2) {
    candle = _.merge(candle, {
      version: candleVersion,
      buyVolume: 0,
      buyTrades: 0,
      lag: this.lag,
      raw: trades
    });
  }

  /**
   * Returns trade type ('buy' or 'sell').
   *
   * If current trade has higher price then the previous one, it must
   * have been a buy action. Or if previous trade was a 'buy' and
   * price hasn't changed, we assume users are still buying.
   */
  var getTradeType = function (trade) {
    var newTradePriceIsHigher = (this.lastTradePrice < trade.price);
    var newTradeRemainsBuy = ((this.lastTradePrice == trade.price) && (this.isBuyAction));

    return (newTradePriceIsHigher || newTradeRemainsBuy) ? 'buy' : 'sell';
  }

  this.lastTradePrice = 0.0;
  this.isBuyAction = false;

  _.each(trades, function(trade) {
    candle.high = _.max([candle.high, f(trade.price)]);
    candle.low = _.min([candle.low, f(trade.price)]);
    candle.volume += f(trade.amount);
    candle.vwp += f(trade.price) * f(trade.amount);

    if(candleVersion === 2) {
      this.isBuyAction = getTradeType(trade) === 'buy';
      this.lastTradePrice = trade.price;

      if (this.isBuyAction) {
        candle.buyVolume += f(trade.amount);
        candle.buyTrades++;
      }
    }
  });

  candle.vwp /= candle.volume;

  return candle;
}

// Gekko expects a candle every minute, if nothing happened
// during a particilar minute Gekko will add empty candles with:
//
// - open, high, close, low, vwp are the same as the close of the previous candle.
// - trades, volume are 0
CandleCreator.prototype.addEmptyCandles = function(candles, candleVersion) {
  var amount = _.size(candles);
  if(!amount)
    return candles;

  // iterator
  var start = _.first(candles).start.clone();
  var end = _.last(candles).start;
  var i, j = -1;

  var minutes = _.map(candles, function(candle) {
    return +candle.start;
  });

  while(start < end) {
    start.add('minute', 1);
    i = +start;
    j++;

    if(_.contains(minutes, i))
      continue; // we have a candle for this minute

    var lastPrice = candles[j].close;

    var emptyCandle = {
      start: start.clone(),
      open: lastPrice,
      high: lastPrice,
      low: lastPrice,
      close: lastPrice,
      vwp: lastPrice,
      volume: 0,
      trades: 0
    };

    if(candleVersion === 2) {
      emptyCandle = _.merge(emptyCandle, {
        version: candleVersion,
        buyVolume: 0,
        buyTrades: 0,
        lag: 0,
        raw: []
      });
    }

    candles.splice(j + 1, 0, emptyCandle);
  }

  return candles;
}

module.exports = CandleCreator;
