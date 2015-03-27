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

/**
 *  Create a transport for FireBase.
 */
var MQTTTransport = function (initd) {
    var self = this;

    self.initd = _.defaults(
        initd,
        iotdb.keystore().get("/transports/MQTTTransport/initd"),
        {
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
    
    self.native = mqtt.createClient(self.initd.port, self.initd.host);
    self.native.on('error', function () {
        logger.info({
            method: "publish/on(error)",
            arguments: arguments
        }, "unexpected");
    });
    self.native.on('clone', function () {
        logger.info({
            method: "publish/on(clone)",
            arguments: arguments
        }, "unexpected");
    });

    self._subscribed = false;
};

/**
 *  List all the IDs associated with this Transport.
 *
 *  The callback is called with a list of IDs
 *  and then null when there are no further values.
 *
 *  Note that this may not be memory efficient due
 *  to the way "value" works. This could be revisited
 *  in the future.
 *
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.list = function(paramd, callback) {
    var self = this;

    if (arguments.length === 1) {
        paramd = {};
        callback = arguments[0];
    }

    callback(null);
};

/**
 *  MQTT: this does nothing, as we don't have 
 *  a concept of a databse
 */
MQTTTransport.prototype.get = function(id, band, callback) {
    var self = this;

    if (!id) {
        throw new Error("id is required");
    }
    if (!band) {
        throw new Error("band is required");
    }

    // don't know (and never will)
    callback(id, band, undefined); 
};

/**
 */
MQTTTransport.prototype.update = function(id, band, value) {
    var self = this;

    if (!id) {
        throw new Error("id is required");
    }
    if (!band) {
        throw new Error("band is required");
    }

    var channel = self._channel(id, band);

    if (self.initd.add_timestamp && !value["@timestamp"]) {
        value = _.shallowCopy(value);
        value["@timestamp"] = (new Date()).toISOString();
        var d = _pack(value);
    } else {
        var d = _pack(value);
    }

    self.native.publish(channel, d, {
        retain: self.initd.retain,
        qos: self.initd.qos,
    });
};

/**
 */
MQTTTransport.prototype.updated = function(id, band, callback) {
    var self = this;

    if (arguments.length === 1) {
        id = null;
        band = null;
        callback = arguments[0];
    } else if (arguments.length === 2) {
        band = null;
        callback = arguments[1];
    }

    if (!self._subscribed) {
        var channel = path.join(self.initd.prefix, "#")
        self.native.subscribe(channel, function(error) {
        });
    }

    self.native.on("message", function(topic, message, packet) {
        var subpath = topic.substring(self.initd.prefix.length).replace(/^\//, '');
        var parts = subpath.split("/");
        if (parts.length !== 2) {
            return;
        }

        var topic_id = _decode(parts[0]);
        var topic_band = _decode(parts[1]);

        if (id && (topic_id !== id)) {
            return;
        }
        if (band && (topic_band !== band)) {
            return;
        }

        var d = _unpack(message);
        callback(topic_id, topic_band, d);
    });
};

/**
 *  MQTT - do nothing
 */
MQTTTransport.prototype.remove = function(id, band) {
    var self = this;

    if (!id) {
        throw new Error("id is required");
    }
};

/* -- internals -- */
MQTTTransport.prototype._channel = function(id, band, paramd) {
    var self = this;

    paramd = _.defaults(paramd, {});

    var channel = self.initd.prefix;
    if (id) {
        channel = path.join(channel, _encode(id));

        if (band) {
            channel = path.join(channel, _encode(band));
        }
    }

    return channel;
};

var _encode = function(s) {
    return s.replace(/[\/#+]/g, function(c) {
        return '%' + c.charCodeAt(0).toString(16);
    });
};

var _decode = function(s) {
    return decodeURIComponent(s);
}

var _unpack = function(d) {
    if (d.toString) {
        d = d.toString();
    }

    return JSON.parse(d);
};

var _pack = function(d) {
    return JSON.stringify(d);
};

/**
 *  API
 */
exports.MQTTTransport = MQTTTransport;
