var chai = require('chai');
var expect = chai.expect;
var should = chai.should;
var sinon = require('sinon');

var _ = require('lodash');
var moment = require('moment');

var utils = require(__dirname + '/../core/util');
var config = utils.getConfig();

var dirs = utils.dirs();
var CandleBatcher = require(dirs.core + 'candleBatcher');

var candles = [
  {"start":moment("2015-02-14T23:57:00.000Z"),"open":257.19,"high":257.19,"low":257.18,"close":257.18,"vwp":257.18559990418294,"volume":0.97206065,"trades":2},
  {"start":moment("2015-02-14T23:58:00.000Z"),"open":257.02,"high":257.02,"low":256.98,"close":256.98,"vwp":257.0175849772836,"volume":4.1407478,"trades":2},
  {"start":moment("2015-02-14T23:59:00.000Z"),"open":256.85,"high":256.99,"low":256.85,"close":256.99,"vwp":256.9376998467,"volume":6,"trades":6},
  {"start":moment("2015-02-15T00:00:00.000Z"),"open":256.81,"high":256.82,"low":256.81,"close":256.82,"vwp":256.815,"volume":4,"trades":2},
  {"start":moment("2015-02-15T00:01:00.000Z"),"open":256.81,"high":257.02,"low":256.81,"close":257.01,"vwp":256.94666666666666,"volume":6,"trades":3},
  {"start":moment("2015-02-15T00:02:00.000Z"),"open":257.03,"high":257.03,"low":256.33,"close":256.33,"vwp":256.74257263558013,"volume":6.7551178,"trades":6},
  {"start":moment("2015-02-15T00:03:00.000Z"),"open":257.02,"high":257.47,"low":257.02,"close":257.47,"vwp":257.26466004728906,"volume":3.7384995300000003,"trades":3},
  {"start":moment("2015-02-15T00:04:00.000Z"),"open":257.47,"high":257.48,"low":257.37,"close":257.38,"vwp":257.4277429116875,"volume":8,"trades":6},
  {"start":moment("2015-02-15T00:05:00.000Z"),"open":257.38,"high":257.45,"low":257.38,"close":257.45,"vwp":257.3975644932184,"volume":7.97062564,"trades":4},
  {"start":moment("2015-02-15T00:06:00.000Z"),"open":257.46,"high":257.48,"low":257.46,"close":257.48,"vwp":257.47333333333336,"volume":7.5,"trades":4}
];

var version2Candles = [
  {"start":moment("2015-02-14T23:57:00.000Z"),"open":257.19,"high":257.19,"low":257.18,"close":257.18,"vwp":257.18559990418294,"volume":0.97206065,"trades":2,"buyVolume":0.97206065,buyTrades:2,lag:100,raw:[{"a":1}]},
  {"start":moment("2015-02-14T23:58:00.000Z"),"open":257.02,"high":257.02,"low":256.98,"close":256.98,"vwp":257.0175849772836,"volume":4.1407478,"trades":2,"buyVolume":2.0,buyTrades:1,lag:200,raw:[{"b":1}]}
];

describe('core/candleBatcher', function () {
  var cb;

  it('should throw when not passed an object', function() {
    expect(function() {
      new CandleBatcher();
    }).to.throw('missing options parameter');
  });

  it('should throw when not passed a candleSize in object', function() {
    expect(function() {
      new CandleBatcher({});
    }).to.throw('candleSize is not a number');
  });

  it('should instantiate without candleVersion value', function () {
    cb = new CandleBatcher({ candleSize: 2 });
    expect(cb.candleVersion).to.be.equal(1);
  });

  it('should instantiate with candleVersion value undefined', function () {
    cb = new CandleBatcher({candleSize:2,candleVersion:undefined});
    expect(cb.candleVersion).to.be.equal(1);
  });

  it('should instantiate with candleVersion value null', function () {
    cb = new CandleBatcher({candleSize:2,candleVersion:null});
    expect(cb.candleVersion).to.be.equal(1);
  });

  it('should instantiate v2 candleVersion', function () {
    cb = new CandleBatcher({candleSize:2, candleVersion:2});
    expect(cb.candleVersion).to.be.equal(2);
  });

  it('should instantiate', function () {
    cb = new CandleBatcher({candleSize:2, candleVersion:1});
    expect(cb.candleVersion).to.be.equal(1);
  });

  it('should throw when fed a candle', function () {
    var candle = _.first(candles);
    expect(
      cb.write.bind(cb, candle)
    ).to.throw('candles is not an array');
  });

  it('should not emit an event when fed not enough candles', function() {
    var candle = _.first(candles);

    var spy = sinon.spy();
    cb.on('candle', spy);
    cb.write( [candle] );
    expect(spy.called).to.be.false;
  });

  it('should emit 5 events when fed 10 candles', function() {
    cb = new CandleBatcher({candleSize:2, candleVersion:1});

    var spy = sinon.spy();
    cb.on('candle', spy);
    cb.write( candles );
    expect(spy.callCount).to.equal(5);
  });

  it('should correctly add two candles together', function() {
    cb = new CandleBatcher({candleSize:2, candleVersion:1});
    var _candles = _.first(candles, 2);
    var first = _.first(_candles);
    var second = _.last(_candles);

    var result = {
      start: first.start,
      open: first.open,
      high: _.max([first.high, second.high]),
      low: _.min([first.low, second.low]),
      close: second.close,
      volume: first.volume + second.volume,
      vwp: (first.vwp * first.volume) + (second.vwp * second.volume),
      trades: first.trades + second.trades
    };

    result.vwp /= result.volume;

    var spy = sinon.spy();
    cb.on('candle', spy);
    cb.write( _candles );

    var cbResult = _.first(_.first(spy.args));
    expect(cbResult).to.deep.equal(result);

  });

  it('should correctly add two v2 candles together', function () {
    cb = new CandleBatcher({candleSize:2, candleVersion:2});

    var _candles = _.first(version2Candles, 2);
    var first = _.first(_candles);
    var second = _.last(_candles);

    var result = {
      start: first.start,
      open: first.open,
      high: _.max([first.high, second.high]),
      low: _.min([first.low, second.low]),
      close: second.close,
      volume: first.volume + second.volume,
      vwp: (first.vwp * first.volume) + (second.vwp * second.volume),
      trades: first.trades + second.trades,
      buyVolume: first.buyVolume + second.buyVolume,
      buyTrades: first.buyTrades + second.buyTrades,
      lag: _.max([first.lag, second.lag]),
      raw: first.raw.concat(second.raw)
    };

    result.vwp /= result.volume;

    var spy = sinon.spy();
    cb.on('candle', spy);
    cb.write( _candles );

    var cbResult = _.first(_.first(spy.args));

    expect(cbResult).to.deep.equal(result);

  });
});