/*
 *  connect_normal.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
 *
 *  Make a HTTP MQTT connect
 */

"use strict";

const iotdb = require('iotdb');
const _ = iotdb._;
const connect = require("../connect");

connect.connect({
    host: "mqtt.iotdb.org",
    verbose: true,
}, (error, connect) => {
    if (error) {
        return console.log("#", _.error.message(error));
    }

    console.log("start");
})
