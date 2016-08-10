/*
 *  connect_listen.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
 *
 *  Make a HTTP MQTT connect and listen to
 *  everything (well, JSON messages)
 */

"use strict";

const iotdb = require('iotdb');
const _ = iotdb._;
const connect = require("../connect");

connect.connect({
    host: "mqtt.iotdb.org",
    verbose: true,
}, (error, mqtt_client) => {
    if (error) {
        return console.log("#", _.error.message(error));
    }

    console.log("start");

    mqtt_client.on("message", (topic, message, packet) => {
        try {
            message = JSON.parse(message.toString());
        } catch (x) {
            return;
        }
        console.log("--");
        console.log("+", "topic", topic);
        console.log("+", "message", message);
    });

    mqtt_client.subscribe("#", (error) => {
    });
})
