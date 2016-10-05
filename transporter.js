/*
 *  transporter.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-10
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

const iotdb = require('iotdb');
const _ = iotdb._;
const iotdb_transport = require('iotdb-transport');
const errors = require('iotdb-errors');

const assert = require('assert');

const connect = require("./connect");

const logger = iotdb.logger({
    name: 'iotdb-transport-mqtt',
    module: 'transporter',
});

const make = (initd, mqtt_client) => {
    const self = iotdb_transport.make();
    self.name = "iotdb-transport-mqtt";

    const _mqtt_client = mqtt_client;
    assert.ok(_mqtt_client);

    const _initd = _.d.compose.shallow(
        initd, {
            channel: iotdb_transport.channel,
            unchannel: iotdb_transport.unchannel,
            encode: s => s.replace(/[\/$%#.\]\[]/g, (c) => '%' + c.charCodeAt(0).toString(16)),
            decode: s => decodeURIComponent(s),
            unpack: (doc, d) => JSON.parse(doc.toString ? doc.toString() : doc),
            pack: d => JSON.stringify(d.value),
        },
        iotdb.keystore().get("/transports/iotdb-transport-mqtt/initd"), {
            prefix: "/",
        }
    );

    const _timestampd = {};

    self.rx.put = (observer, d) => {
        _mqtt_client.ensure(error => {
            if (_.is.Error(error)) {
                return observer.onError(error);
            }

            const topic = _initd.channel(_initd, d);
            const message = _initd.pack(_initd, d);

            if (_initd.verbose) {
                logger.info({
                    topic: topic,
                    message: message,
                    message_type: typeof message,
                }, "VERBOSE: sending message");
            }

            /*
            const timestamp = d.value["@timestamp"];
            if (timestamp) {
                _timestampd[topic] = timestamp;
            }
            */

            _mqtt_client.publish(topic, message, {
                retain: _initd.retain,
                qos: _initd.qos,
            }, error => {
                if (_.is.Error(error)) {
                    return observer.onError(error);
                }

                observer.onNext(_.d.clone.shallow(d));
                observer.onCompleted();
            });
        });
    };

    let _subscribed = false;
    
    self.rx.updated = (observer, d) => {
        _mqtt_client.ensure(error => {
            if (error) {
                logger.error({
                    method: "updated/_mqtt_client",
                    cause: "likely MQTT issue - see previous error messages",
                }, "could not get MQTT client");
                return;
            }

            if (!_subscribed) {
                _subscribed = true;

                const channel = _initd.channel(initd, {
                    id: "#"
                });
                _mqtt_client.subscribe(channel, error => {
                    if (error) {
                        logger.error({
                            method: "updated/mqtt.subscribe",
                            cause: "likely MQTT issue - this is probably very bad",
                            channel: channel,
                        }, "unexpected error subscribing");
                    }
                });
            }

            _mqtt_client.on("message", (topic, message, packet) => {
                const md = _initd.unchannel(initd, topic, message);

                if (d.id && (md.id !== d.id)) {
                    return;
                }
                if (d.band && (md.band !== d.band)) {
                    return;
                }

                const rd = _.d.clone.shallow(d);
                rd.id = md.id;
                rd.band = md.band;
                rd.value = _initd.unpack(message, md);

                /*
                const timestamp = rd.value["@timestamp"];
                if (timestamp) {
                    console.log("HERE:XXX.IN.1", topic, _timestampd[topic], timestamp);
                    console.log("HERE:XXX.IN.2", rd);
                }
                */

                // console.log("MQTT.2", topic);
                observer.onNext(rd);
            });
        });
    };

    self.rx.list = (observer, d) => { throw new errors.NeverImplemented(); };
    self.rx.added = (observer, d) => { throw new errors.NeverImplemented(); };
    self.rx.get = (observer, d) => { throw new errors.NeverImplemented(); };
    self.rx.bands = (observer, d) => { throw new errors.NeverImplemented(); };

    return self;
};

/**
 *  API
 */
exports.make = make;

