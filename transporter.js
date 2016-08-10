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
            unpack: (d, id, band) => _.d.transform(d, { pre: _.ld_compact, key: _initd._decode, }),
            pack: (d, id, band) => _.d.transform(d, { pre: _.ld_compact, key: _initd._encode, }),
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

            var topic = _initd.channel(_initd, d.id, d.band);
            var message = _initd.pack(value, d.id, d.band);

            if (_initd.verbose) {
                logger.info({
                    topic: topic,
                    message, message,
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
    
    self.rx.updated = (observer, d) => {
        observer.onCompleted();
    };

    // -- internals
    const _mqtt_ready = (done) => {
        if (_mqtt.connected) {
            done();
        } else {
            _mqtt.once("connect", () => done();
        }
    };

    return self;
};

/**
 *  API
 */
exports.make = make;

