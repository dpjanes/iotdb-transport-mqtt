/*
 *  send.js
 *
 *  David Janes
 *  IOTDB.org
 *  2015-03-27
 *
 *  Demonstrate sending something
 *  Make sure to see README first
 */

var Transport = require('../MQTTTransport').MQTTTransport;

var p = new Transport({
    host: "mqtt.iotdb.org",
    prefix: "/u/mqtt-transport",
    retain: true,
    add_timestamp: true,
});

var _update = function() {
    var now = (new Date()).toISOString();
    console.log("+ sent update", now);
    p.update({
        id: "MyThingID", 
        band: "meta", 
        value: {
            first: "David",
            last: "Janes",
            now: now,
        },
    });
};

setInterval(_update, 10 * 1000);
_update();
