/*
 *  make.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
 */

const iotdb = require('iotdb');
const _ = iotdb._;
const connect = require("../connect");

const transporter = require("../transporter");

const mqtt_client = connect.connect({
    host: "mqtt.iotdb.org",
    verbose: true,
}, (error, mqtt_client) => {
    if (error) {
        return console.log("#", _.error.message(error));
    }
})

const transport = transporter.make({
    prefix: "my/things",
    verbose: true,
}, mqtt_client);

exports.transport = transport;

