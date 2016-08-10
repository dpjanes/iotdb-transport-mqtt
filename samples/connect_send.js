/*
 *  connect_send.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
 *
 *  Make a HTTP MQTT connect and send and listen 
 *  on the same connction.
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

    console.log("-", "connected");

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

    const channel = "/channel/abc";

    mqtt_client.subscribe(channel, (error) => {
    });

    setInterval(() => {
        mqtt_client.publish(
            channel, 
            JSON.stringify(_.timestamp.add({ "message": "hi" })),
            {}, 
            () => console.log("-", "send")
        );
    }, 2500);
})
