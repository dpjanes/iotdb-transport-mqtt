/*
 *  bad_list.js
 *
 *  David Janes
 *  IOTDB.org
 *  2015-03-27
 *
 *  Deal with data that does not exist
 *  Expect to see just 'null'
 */

var Transport = require('../MQTTTransport').MQTTTransport;

var p = new Transport({
    host: "mqtt.iotdb.org",
    prefix: "/u/mqtt-transport",
});
p.list(function(d) {
    console.log(d.id);
});
