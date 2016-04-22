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
var iotdb_transport = require('iotdb-transport');
var errors = iotdb_transport.errors;
var _ = iotdb._;

var url_join = require('url-join');
var path = require('path');
var mqtt = require('mqtt');
var fs = require('fs');

var util = require('util');
var url = require('url');

var logger = iotdb.logger({
    name: 'iotdb-transport-mqtt',
    module: 'MQTTTransport',
});

/* --- forward definitions --- */
var _encode;
var _decode;
var _unpack;
var _pack;


/* --- we generalize this a little so MQTT can be created outside --- */
var _setup_initd = function(initd) {
    return _.d.compose.shallow(
        initd, {
            channel: iotdb_transport.channel,
            unchannel: iotdb_transport.unchannel,
            encode: _encode,
            decode: _decode,
            pack: _pack,
            unpack: _unpack,
            allow_updated: false,
            client_id: "tr-mqtt-" + _.random.id(10),
            user: null,
        },
        iotdb.keystore().get("/transports/MQTTTransport/initd"), {
            verbose: false,

            prefix: "",
            host: "",
            retain: false,
            qos: 0,
            add_timestamp: false,

            // secure conections
            protocol: null,
            port: null,
            ca: null,
            cert: null,
            key: null,
        }
    );
};

var _connect = function(initd) {
    if (!initd.host) {
        throw new Error("MQTTTransport: expected initd.host");
    }

    var connectd = {
        clientId: initd.client_id,
    };

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

    var url = util.format("%s://%s:%s", initd.protocol || "mqtt", initd.host, initd.port);

    if (initd.verbose) {
        logger.info({
            url: url,
            // connectd: connectd,
        }, "VERBOSE: connect info");
    }

    var native = mqtt.connect(url, connectd);
    native.on('connect', function () {
        logger.info({
            method: "publish/on(connect)",
            url: url,
        }, "connected");

        console.log("===============================");
        console.log("=== MQTT Server Connected");
        console.log("=== ");
        console.log("=== Connect at:");
        console.log("=== " + url_join(url, initd.prefix));
        console.log("===============================");
    });

    return native;
};

/**
 *  'done' is optional, but if used you're guarenteed a connection (or error)
 */
var connect = function(initd, done) {
    const client = _connect(_setup_initd(initd));

    done = done || _.noop;

    client.once('connect', function () {
        done(null, client);
        done = _.noop;
    });
    client.once('error', function (error) {
        done(error);
        done = _.noop;
    });

    return client;
};


/* --- constructor --- */

/**
 *  See {iotdb_transport.Transport#Transport} for documentation.
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
 *  @param {boolean} initd.allow_updated
 *  If True, allow MQTT to update data (e.g. by receiving
 *  it over MQTT). Default false
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

    self.initd = _setup_initd(initd);

    if (native) {
        self.native = native;
    } else {
        self.native = _connect(self.initd);
    }

    self.native.on('error', function () {
        logger.error({
            method: "publish/on(error)",
            arguments: arguments,
            url: url,
            cause: "likely MQTT issue - will automatically reconnect soon",
        }, "unexpected error");
    });
    self.native.on('close', function () {
        logger.info({
            method: "publish/on(close)",
            url: url,
            cause: "likely MQTT issue - will automatically reconnect soon",
        }, "unexpected close");
    });

    self._subscribed = false;
};

MQTTTransport.prototype = new iotdb_transport.Transport();
MQTTTransport.prototype._class = "MQTTTransport";

/* --- methods --- */

/**
 *  See {iotdb_transport.Transport#list} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse. 
 */
MQTTTransport.prototype.list = function (paramd, callback) {
    var self = this;
    var ld;

    self._validate_list(paramd, callback);

    ld = _.d.clone.shallow(paramd);
    callback(new errors.NeverImplemented(), ld);
};

/**
 *  See {iotdb_transport.Transport#added} for documentation.
 *  <p>
 *  NOT FINISHED
 */
MQTTTransport.prototype.added = function (paramd, callback) {
    var self = this;

    self._validate_added(paramd, callback);
};

/**
 *  See {iotdb_transport.Transport#bands} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.bands = function (paramd, callback) {
    var self = this;

    self._validate_bands(paramd, callback);

    var bd = _.d.clone.shallow(paramd);

    callback(new errors.NeverImplemented(), bd);
};

/**
 *  See {iotdb_transport.Transport#get} for documentation.
 *  <p>
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.get = function (paramd, callback) {
    var self = this;

    self._validate_get(paramd, callback);

    var gd = _.d.clone.shallow(paramd);
    gd.value = null;

    callback(new errors.NotImplemented(), gd);
};

/**
 *  See {iotdb_transport.Transport#update} for documentation.
 */
MQTTTransport.prototype.put = function (paramd, callback) {
    var self = this;

    self._validate_update(paramd, callback);

    var pd = _.d.clone.shallow(paramd);

    var value = paramd.value;
    var timestamp = value["@timestamp"];
    if (!timestamp && _.is.Boolean(self.initd.add_timestamp)) {
        value = _.d.clone.shallow(value);
        value["@timestamp"] = _.timestamp.make();
    } else if (!timestamp && _.is.Array(self.initd.add_timestamp) && (self.init.add_timestamp.indexOf(paramd.band) > -1)) {
        value = _.d.clone.shallow(value);
        value["@timestamp"] = _.timestamp.make();
    }

    var channel = self.initd.channel(self.initd, paramd.id, paramd.band);
    var d = self.initd.pack(value, paramd.id, paramd.band);

    self._mqtt_client(function(error, mqtt_client) {
        if (error) {
            logger.error({
                method: "put/_mqtt_client",
                cause: "likely MQTT issue - see previous error messages",
            }, "could not get MQTT client");
            return;
        }

        if (self.initd.verbose) {
            logger.info({
                channel: channel,
                d: d,
                retain: self.initd.retain,
                qos: self.initd.qos,
            }, "VERBOSE: sending message");
        }

        mqtt_client.publish(channel, d, {
            retain: self.initd.retain,
            qos: self.initd.qos,
        }, function () {
            callback(null, pd);
        });
    });

};

/**
 *  See {iotdb_transport.Transport#updated} for documentation.
 */
MQTTTransport.prototype.updated = function (paramd, callback) {
    var self = this;

    self._validate_updated(paramd, callback);

    if (!self.initd.allow_updated) {
        return;
    }

    self._mqtt_client(function(error, mqtt_client) {
        if (error) {
            logger.error({
                method: "updated/_mqtt_client",
                cause: "likely MQTT issue - see previous error messages",
            }, "could not get MQTT client");
            return;
        }

        if (!self._subscribed) {
            var channel = self.initd.prefix + "/#";
            self.native.subscribe(channel, function (error) {
                if (error) {
                    logger.error({
                        method: "updated/mqtt.subscribe",
                        cause: "likely MQTT issue - this is probably very bad",
                    }, "unexpected error subscribing");
                }
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
            callback(null, {
                id: topic_id,
                band: topic_band,
                value: d,
            });
        });
    });
};

/**
 *  See {iotdb_transport.Transport#remove} for documentation.
 *  <p>
 *  MQTT - do nothing
 */
MQTTTransport.prototype.remove = function (paramd, callback) {
    var self = this;

    self._validate_remove(paramd, callback);

    var rd = _.d.clone.shallow(paramd);
    delete rd.band;
    delete rd.value;

    callback(new errors.NeverImplemented(), rd);
};

/**
 *  Return a _connected_ MQTT client
 */
MQTTTransport.prototype._mqtt_client = function (callback) {
    var self = this;

    if (!self.native)  {
        logger.error({
            method: "_mqtt_client",
            cause: "likely programming issue",
        }, "no MQTT client?");

        return callback(new errors.Internal("no MQTT client"));
    }

    if (self.native.ready) {
        callback(null, self.native);
    } else {
        self.native.once("connect", function() {
            callback(null, self.native);
        });
    }
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
exports.connect = connect;
