/*
 *  list.js
 *
 *  David Janes
 *  IOTDB.org
 *  2015-03-27
 *
 *  Demonstrate receiving
 *  Make sure to see README first
 */

var Transport = require('../MQTTTransport').MQTTTransport;

var p = new Transport({
    host: "mqtt.iotdb.org",
    prefix: "/u/mqtt-transport",
});
p.list(function(id) {
    console.log(id);
});
