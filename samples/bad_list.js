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
p.list(function(error, ld) {
    if (error) {
        console.log("#", "error", error);
        return;
    }
    if (!ld) {
        console.log("+", "<end>");
        break;
    }

    console.log("+", ld.id);
});
