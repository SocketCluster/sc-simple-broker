const AsyncStreamEmitter = require('async-stream-emitter');
const StreamDemux = require('stream-demux');
var SCChannel = require('sc-channel');

function SimpleExchange(broker) {
  AsyncStreamEmitter.call(this);

  this._broker = broker;
  this._channelMap = {};
  this._channelEventDemux = new StreamDemux();
  this._channelDataDemux = new StreamDemux();

  (async () => {
    for await (let {channel, data} of this._broker.listener('message')) {
      this._channelDataDemux.write(channel, data);
    }
  })();
}

SimpleExchange.prototype = Object.create(AsyncStreamEmitter.prototype);

SimpleExchange.prototype.destroy = function () {
  this._broker.closeAllListeners();
};

SimpleExchange.prototype._triggerChannelSubscribe = function (channel) {
  let channelName = channel.name;

  channel.state = SCChannel.SUBSCRIBED;

  this._channelEventDemux.write(`${channelName}/subscribe`, {});
  this.emit('subscribe', {channel: channelName});
};

SimpleExchange.prototype._triggerChannelUnsubscribe = function (channel) {
  let channelName = channel.name;

  delete this._channelMap[channelName];
  if (channel.state === SCChannel.SUBSCRIBED) {
    this._channelEventDemux.write(`${channelName}/unsubscribe`, {});
    this.emit('unsubscribe', {channel: channelName});
  }
};

SimpleExchange.prototype.publish = function (channelName, data) {
  return this._broker.publish(channelName, data);
};

SimpleExchange.prototype.subscribe = function (channelName) {
  let channel = this._channelMap[channelName];

  if (!channel) {
    channel = {
      name: channelName,
      state: SCChannel.PENDING
    };
    this._channelMap[channelName] = channel;
    this._triggerChannelSubscribe(channel);
  }

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new SCChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SimpleExchange.prototype.unsubscribe = function (channelName) {
  let channel = this._channelMap[channelName];

  if (channel) {
    this._triggerChannelUnsubscribe(channel);
  }
};

SimpleExchange.prototype.channel = function (channelName) {
  let currentChannel = this._channelMap[channelName];

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new SCChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SimpleExchange.prototype.getChannelState = function (channelName) {
  let channel = this._channelMap[channelName];
  if (channel) {
    return channel.state;
  }
  return SCChannel.UNSUBSCRIBED;
};

SimpleExchange.prototype.getChannelOptions = function (channelName) {
  return {};
};

SimpleExchange.prototype.subscriptions = function (includePending) {
  let subs = [];
  Object.keys(this._channelMap).forEach((channelName) => {
    if (includePending || this._channelMap[channelName].state === SCChannel.SUBSCRIBED) {
      subs.push(channelName);
    }
  });
  return subs;
};

SimpleExchange.prototype.isSubscribed = function (channelName, includePending) {
  let channel = this._channelMap[channelName];
  if (includePending) {
    return !!channel;
  }
  return !!channel && channel.state === SCChannel.SUBSCRIBED;
};


function SCSimpleBroker() {
  AsyncStreamEmitter.call(this);

  this.isReady = false;
  this._exchangeClient = new SimpleExchange(this);
  this._clientSubscribers = {};
  this._clientSubscribersCounter = {};

  setTimeout(() => {
    this.isReady = true;
    this.emit('ready', {});
  }, 0);
}

SCSimpleBroker.prototype = Object.create(AsyncStreamEmitter.prototype);

SCSimpleBroker.prototype.exchange = function () {
  return this._exchangeClient;
};

SCSimpleBroker.prototype.subscribeSocket = function (socket, channel) {
  if (!this._clientSubscribers[channel]) {
    this._clientSubscribers[channel] = {};
    this._clientSubscribersCounter[channel] = 0;
  }
  if (!this._clientSubscribers[channel][socket.id]) {
    this._clientSubscribersCounter[channel]++;
  }
  this._clientSubscribers[channel][socket.id] = socket;
  return Promise.resolve();
};

SCSimpleBroker.prototype.unsubscribeSocket = function (socket, channel) {
  if (this._clientSubscribers[channel]) {
    if (this._clientSubscribers[channel][socket.id]) {
      this._clientSubscribersCounter[channel]--;
      delete this._clientSubscribers[channel][socket.id];

      if (this._clientSubscribersCounter[channel] <= 0) {
        delete this._clientSubscribers[channel];
        delete this._clientSubscribersCounter[channel];
      }
    }
  }
  return Promise.resolve();
};

SCSimpleBroker.prototype.publish = function (channelName, data) {
  this._handleExchangeMessage({
    channel: channelName,
    data
  });
  return Promise.resolve();
};

SCSimpleBroker.prototype._handleExchangeMessage = function (packet) {
  var subscriberSockets = this._clientSubscribers[packet.channel] || {};

  Object.keys(subscriberSockets).forEach((i) => {
    subscriberSockets[i].transmit('#publish', packet);
  });

  this.emit('message', packet);
};

module.exports.SCSimpleBroker = SCSimpleBroker;
