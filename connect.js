/*
 *  connect.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
 *
 *  Make a MQTT server
 *
 *  Copyright [2013-2016] [David P. Janes]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

"use strict";

// require('longjohn')

const assert = require("assert");

const iotdb = require('iotdb');
const _ = iotdb._;

const iotdb_transport = require('iotdb-transport');
const errors = require('iotdb-errors');

const path = require('path');
const mqtt = require('mqtt');
const fs = require('fs');

const util = require('util');

const logger = iotdb.logger({
    name: 'iotdb-transport-mqtt',
    module: 'server',
});

const _setup_initd = initd => _.d.compose.shallow(
    initd,
    iotdb.keystore().get("/transports/iotdb-transport-mqtt/initd"), {
        verbose: false,

        prefix: "",
        host: "",
        retain: false,
        qos: 0,
        add_timestamp: false,

        // login
        username: null,
        password: null,
        client_id: null,

        // secure conections
        protocol: null,
        port: null,
        ca: null,
        cert: null,
        key: null,
    });

const _connect = initd => {
    assert.ok(initd.host, "expected initd.host");

    const connectd = {
        clientId: initd.client_id,
    };

    if (initd.username) {
        connectd.username = initd.username;
    }

    if (initd.password) {
        connectd.password = initd.password;
    }

    if (initd.key) {
        connectd.key = fs.readFileSync(initd.key);
    }

    if (initd.cert) {
        connectd.cert = fs.readFileSync(initd.cert);
    }

    if (initd.ca) {
        connectd.ca = fs.readFileSync(initd.ca);
    }

    if (!initd.protocol) {
        if (connectd.key && connectd.cert) {
            initd.protocol = 'mqtts';
        } else {
            initd.protocol = 'mqtt';
        }
    }

    if (!initd.port) {
        if (initd.protocol === 'mqtts') {
            initd.port = 8883;
        } else {
            initd.port = 1883;
        }
    }

    initd.url = util.format("%s://%s:%s", initd.protocol || "mqtt", initd.host, initd.port);

    if (initd.verbose) {
        logger.info({
            url: initd.url,
            // connectd: connectd,
        }, "VERBOSE: connect info");
    }

    const native = mqtt.connect(initd.url, connectd);
    native.on('connect', () => {
        logger.info({
            method: "_connect/on(connect)",
            url: initd.url,
        }, "connected");

        console.log("===============================");
        console.log("=== MQTT Server Connected");
        console.log("=== ");
        console.log("=== Connect at:");
        console.log("=== " + _.net.url.join(initd.url, initd.prefix));
        console.log("===============================");
    });
    native.on('disconnect', () => {
        logger.warn({
            method: "_connect/on(disconnect)",
        }, "MQTT disconncted");
    });
    native.on('error', error => {
        logger.warn({
            method: "_connect/on(error)",
            error: _.error.message(error),
        }, "MQTT error");
    });

    return native;
};

const connect = (initd, done) => {
    const client = _connect(_setup_initd(initd));

    client.once('connect', () => {
        done(null, client);
        done = _.noop;
    });
    client.once('error', (error) => {
        done(error);
        done = _.noop;
    });

    client.ensure = (done) => {
        if (client.connected) {
            done();
        } else {
            client.once("connect", () => done());
        }
    };

    return client;
};

/**
 *  API
 */
exports.connect = connect;
