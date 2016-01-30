/*
 *  aws_wildcard.js
 *
 *  David Janes
 *  IOTDB.org
 *  2016-01-30
 *
 *  Demonstrate receiving messages from AWS
 */

var MQTTTransport = require('../MQTTTransport').MQTTTransport;

var p = new MQTTTransport({
    host: "A1GOKL7JWGA91X.iot.us-east-1.amazonaws.com",
    prefix: "iotdb/homestar/0/81EA6324-418D-459C-A9C4-D430F30021C7/alexa",
    ca: "certs/rootCA.pem",
    cert: "certs/cert.pem",
    key: "certs/private.pem",
    allow_updated: true,
});

p.updated({}, function(error, ud) {
    if (error) {
        console.log("#", error);
        return;
    }

    console.log("+", ud);

    /*
    if (value === undefined) {
        p.get(id, band, function(_id, _band, value) {
            if (error) {
                console.log("#", error);
                return;
            }
            console.log("+", id, band, value);
        });
    } else {
        console.log("+", id, band, value);
    }
    */
});
