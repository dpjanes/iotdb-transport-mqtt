/*
 *  transporter.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-08-05
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

const Rx = require('rx');
const events = require('events');
const assert = require('assert');

const connect = require("./connect");

const logger = iotdb.logger({
    name: 'iotdb-transport-fs',
    module: 'transporter',
});

const make = (initd, mqtt) => {
    const self = iotdb_transport.make();

    const _mqtt = mqtt;
    assert.ok(_mqtt);

    const _initd = _.d.compose.shallow(
        initd, {
            channel: iotdb_transport.channel,
            unchannel: iotdb_transport.unchannel,
            encode: s => s.replace(/[\/$%#.\]\[]/g, (c) => '%' + c.charCodeAt(0).toString(16)),
            decode: s => decodeURIComponent(s),
            unpack: (d, id, band) => JSON.parse(d.toString ? d.toString() : d),
            pack: (d, id, band) => JSON.stringify(d),
        },
        iotdb.keystore().get("/transports/MQTTTransport/initd"), {
            prefix: "/",
        }
    );

    self.rx.put = (observer, d) => {
        _mqtt_ready(error => {
            if (error) {
                return observer.onError(error);
            }

            const topic = _initd.channel(_initd, d.id, d.band);
            const message = _initd.pack(d.value, d.id, d.band);

            if (_initd.verbose) {
                logger.info({
                    topic: topic,
                    message: message,
                    message_type: typeof message,
                }, "VERBOSE: sending message");
            }

            _mqtt.publish(topic, message, {
                retain: _initd.retain,
                qos: _initd.qos,
            }, error => {
                if (error) {
                    return observer.onError(error);
                }

                observer.onNext(_.d.clone.shallow(d));
                observer.onCompleted();
            });
        });
    };

    let _subscribed = false;
    
    self.rx.updated = (observer, d) => {
        _mqtt_ready(error => {
            if (error) {
                logger.error({
                    method: "updated/_mqtt_client",
                    cause: "likely MQTT issue - see previous error messages",
                }, "could not get MQTT client");
                return;
            }

            if (!_subscribed) {
                _subscribed = true;

                const channel = _initd.channel(initd, "#");
                _mqtt.subscribe(channel, error => {
                    if (error) {
                        logger.error({
                            method: "updated/mqtt.subscribe",
                            cause: "likely MQTT issue - this is probably very bad",
                            channel: channel,
                        }, "unexpected error subscribing");
                    }
                });
            }

            _mqtt.on("message", (topic, message, packet) => {
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
                rd.value = _initd.unpack(message, md.id, md.band);

                observer.onNext(rd);
            });
        });
    };

    // -- internals
    const _mqtt_ready = done => {
        if (_mqtt.connected) {
            done();
        } else {
            _mqtt.once("connect", () => done());
        }
    };

    return self;
};

/**
 *  API
 */
exports.make = make;

