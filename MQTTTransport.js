/*
 *  MQTTTransport.js
 *
 *  David Janes
 *  IOTDB.org
 *  2015-03-27
 *
 *  Copyright [2013-2015] [David P. Janes]
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

var iotdb = require('iotdb');
var _ = iotdb._;
var bunyan = iotdb.bunyan;

var path = require('path');
var mqtt = require('mqtt');

var util = require('util');
var url = require('url');

var logger = bunyan.createLogger({
    name: 'iotdb-transport-mqtt',
    module: 'MQTTTransport',
});

/* --- forward definitions --- */
var _encode;
var _decode;
var _unpack;
var _pack;

/* --- constructor --- */

/**
 *  See {iotdb.transporter.Transport#Transport} for documentation.
 *
 *  @param {dictionary} initd
 *
 *  @param {string} initd.prefix
 *  MQTT topic prefix
 *
 *  @param {string} initd.host
 *  MQTT server host
 *
 *  @param {integer} initd.port
 *  MQTT server port, by default 1883
 *
 *  @param {boolean} initd.retain
 *  Make messages retained, by default false
 *
 *  @param {boolean} initd.qos
 *  MQTT QOS. Either 0, 1 or 2. By default 0.
 *
 *  @param {boolean|array} initd.add_timestamp
 *  Add a @timestamp to outgoing records.
 *  If an array, the band must be in the array.
 *
 *  @param {function} initd.channel
 *  @param {function} initd.unchannel
 *
 *  @param {MQTTClient|undefined} native
 *  If defined, this will be used for the MQTT client / connection.
 *  Otherwise we will make our own.
 */
var MQTTTransport = function (initd, native) {
    var self = this;

    self.initd = _.defaults(
        initd, {
            channel: iotdb.transporter.channel,
            unchannel: iotdb.transporter.unchannel,
            encode: _encode,
            decode: _decode,
            pack: _pack,
            unpack: _unpack,
        },
        iotdb.keystore().get("/transports/MQTTTransport/initd"), {
            prefix: "",
            host: "",
            port: 1883,
            retain: false,
            qos: 0,
            add_timestamp: false,
        }
    );

    if (!self.initd.host) {
        throw new Error("MQTTTransport: expected initd.host");
    }

    if (native) {
        self.native = native;
    } else {
        self.native = mqtt.createClient(self.initd.port, self.initd.host);
    }

    self.native.on('error', function () {
        logger.error({
            method: "publish/on(error)",
            arguments: arguments,
            cause: "likely MQTT issue - will automatically reconnect soon",
        }, "unexpected error");
    });
    self.native.on('close', function () {
        logger.error({
            method: "publish/on(close)",
            arguments: arguments,
            cause: "likely MQTT issue - will automatically reconnect soon",
        }, "unexpected close");
    });

    self._subscribed = false;
};

MQTTTransport.prototype = new iotdb.transporter.Transport();

/* --- methods --- */

/**
 *  See {iotdb.transporter.Transport#list} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse. 
 */
MQTTTransport.prototype.list = function (paramd, callback) {
    var self = this;

    if (arguments.length === 1) {
        paramd = {};
        callback = arguments[0];
    }

    self._validate_list(paramd, callback);

    callback({
        end: true,
        error: new Error("N/A"),
    });
};

/**
 *  See {iotdb.transporter.Transport#added} for documentation.
 *  <p>
 *  NOT FINISHED
 */
MQTTTransport.prototype.added = function (paramd, callback) {
    var self = this;

    if (arguments.length === 1) {
        paramd = {};
        callback = arguments[0];
    }

    self._validate_added(paramd, callback);
};

/**
 *  See {iotdb.transporter.Transport#about} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.about = function (paramd, callback) {
    var self = this;

    self._validate_about(paramd, callback);

    // don't know (and never will)
    callback({
        id: paramd.id,
        band: paramd.band,
        value: undefined,
        error: new Error("N/A"),
    });
};

/**
 *  See {iotdb.transporter.Transport#get} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.get = function (paramd, callback) {
    var self = this;

    self._validate_get(paramd, callback);

    // don't know (and never will)
    callback({
        id: paramd.id,
        band: paramd.band,
        value: undefined,
        error: new Error("N/A"),
    });
};

/**
 *  See {iotdb.transporter.Transport#update} for documentation.
 */
MQTTTransport.prototype.update = function (paramd, callback) {
    var self = this;

    self._validate_update(paramd, callback);

    var value = paramd.value;
    var timestamp = value["@timestamp"];
    if (!timestamp && _.is.Boolean(self.initd.add_timestamp)) {
        value = _.shallowCopy(value);
        value["@timestamp"] = _.timestamp();
    } else if (!timestamp && _.is.Array(self.initd.add_timestamp) && (self.init.add_timestamp.indexOf(paramd.band) > -1)) {
        value = _.shallowCopy(value);
        value["@timestamp"] = _.timestamp();
    }

    var channel = self.initd.channel(self.initd, paramd.id, paramd.band);
    var d = self.initd.pack(value, paramd.id, paramd.band);

    self.native.publish(channel, d, {
        retain: self.initd.retain,
        qos: self.initd.qos,
    });
};

/**
 *  See {iotdb.transporter.Transport#updated} for documentation.
 */
MQTTTransport.prototype.updated = function (paramd, callback) {
    var self = this;

    if (arguments.length === 1) {
        paramd = {};
        callback = arguments[0];
    }

    self._validate_updated(paramd, callback);

    if (!self._subscribed) {
        var channel = path.join(self.initd.prefix, "#");
        self.native.subscribe(channel, function (error) {
            /* maybe reset _subscribed on mqtt.open? */
            logger.error({
                method: "publish/on(close)",
                arguments: arguments,
                cause: "likely MQTT issue - this is probably very bad",
            }, "unexpected error subscribing");
        });
    }

    self.native.on("message", function (topic, message, packet) {
        var parts = self.initd.unchannel(self.initd, topic);
        if (!parts) {
            return;
        }

        var topic_id = parts[0];
        var topic_band = parts[1];

        if (paramd.id && (topic_id !== paramd.id)) {
            return;
        }
        if (paramd.band && (topic_band !== paramd.band)) {
            return;
        }

        var d = self.initd.unpack(message, topic_id, topic_band);
        callback({
            id: topic_id,
            band: topic_band,
            value: d,
        });
    });
};

/**
 *  See {iotdb.transporter.Transport#remove} for documentation.
 *  <p>
 *  MQTT - do nothing
 */
MQTTTransport.prototype.remove = function (paramd, callback) {
    var self = this;

    self._validate_remove(paramd, callback);
};

/* -- internals -- */
var _encode = function (s) {
    return s.replace(/[\/#+]/g, function (c) {
        return '%' + c.charCodeAt(0).toString(16);
    });
};

var _decode = function (s) {
    return decodeURIComponent(s);
};

var _unpack = function (d) {
    if (d.toString) {
        d = d.toString();
    }

    return JSON.parse(d);
};

var _pack = function (d) {
    return JSON.stringify(d);
};

/**
 *  API
 */
exports.MQTTTransport = MQTTTransport;
