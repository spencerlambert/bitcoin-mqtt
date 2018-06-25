const zmq = require('zeromq');
const sock = zmq.socket('sub');
const bch = require('bitcoincashjs');

const request = require('request');
const timers = require('timers');

const redis = require('redis'),
  redclient = redis.createClient();

// Open ZeroMQ channel
sock.connect('tcp://10.138.176.206:28332');
sock.subscribe('rawtx');
console.log('Subscriber connected to port 28332');
 
sock.on('message', function(topic, message) {
  let tx = bch.Transaction(message.toString('hex')).toObject();

  // Check if this TX has already been seen
  redclient.get(tx.hash, function(err, res) {
    if (err) {
      console.log(err);
      return
    }
    if (res) return; // This TX has already been published

    // Check TX outputs for a match to any MQTT channels
    for (let key in tx.outputs) {
      let pubkey = false;
      let amt = tx.outputs[key].satoshis;
      let scpt = bch.Script(tx.outputs[key].script);
      // Only new bitcoin cash hash addresses are supported
      if (scpt.isPublicKeyHashOut())
        pubkey = bch.Address(scpt.getPublicKeyHash()).toString('cashaddr');
      if (pubkey !== false) {
        // See if someone is watching this address on MQTT for incomming transactions
        redclient.hget(pubkey, "bchTxAmtIn", function (err, res) {
          if (err) {
            console.log(err);
            return;
          }
          if (res === null) return;  // Nothing to check

          // Mark TX as being published, so that duplicates aren't sent out.
          // There is a very small chance that if a duplicate TX was sent over ZMQ rapidly
          // it would get published more than once.
          // It looks like ZMQ is sending the TX once at zero confirmations and again when 
          // included in a block.
          redclient.set(tx.hash, true);
          console.log("Tx In:", res, amt);
          pubMsg(res, amt);
        });
      }
    }
  });

});

function pubMsg(topic, msg) {

  let options = {
    uri: 'http://10.138.120.214:8080/api/v2/mqtt/publish',
    body: JSON.stringify({
      topic: topic,
      payload: msg.toString(),
      qos: 0,
      retain: false,
      client_id: 'api'
    })
  };

  request.post(options, function(err, res, body) {
    if (err) {
      console.log(err);
      return;
    }
    console.log(body);
  })
  .auth('admin','public', false);

}


const bchTxAmtIn = /^bch\/tx\/amt\/in\//g;

function updateMqttChannels() {

  // TODO: add support for pages - http://emqtt.io/docs/v2/rest.html#routes
  // TODO: It might be best to track this list via the webhooks plugin - https://github.com/emqtt/emq-web-hook
  request.get('http://10.138.120.214:8080/api/v2/routes', function(err, res, body) {
    if (err) {
      console.log(err);
      return;
    }
    for (let key in body.result.objects) {
      // Add address and channel to redis
      if (bchTxAmtIn.test(body.result.objects[key].topic))
        redclient.hset(body.result.objects[key].topic.split(bchTxAmtIn)[1], "bchTxAmtIn", body.result.objects[key].topic);
    }
  })
  .auth('admin','public', false)
  .json();
  
}

timers.setInterval(updateMqttChannels, 1000); // Run every second

