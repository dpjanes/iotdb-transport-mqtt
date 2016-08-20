# iotdb-transport-mqtt
IOTDB MQTT Transport

<img src="https://raw.githubusercontent.com/dpjanes/iotdb-homestar/master/docs/HomeStar.png" align="right" />

# Introduction

Read about Transporters [here](https://github.com/dpjanes/iotdb-transport).

This Transporter will transport data from and to MQTT. Note that reading
commands like `list`, `get`, `bands` are meaningless as data only exists
when it shows up on the wire: this Transport does not retain anything.

# Use

See the samples folder for working examples

## Broadcasting

Get another transporter as a "source" - typically this will be IOTDB.
In this particular example, we will connect to a WeMoSocket on the network.

    const iotdb = require("iotdb");
    iotdb.use("homestar-wemo");
    
    const things = iotdb.connect("WeMoSocket");

    const iotdb_transport = require("iotdb-transport-iotdb");
    const iotdb_transporter = iotdb_transport.make({}, things);

Create a MQTT client instance. We provide a helper for this.

    const mqtt_transport = require("iotdb-transport-mqtt");

    const mqtt_client = mqtt_transport.connect({
        host: "mqtt.iotdb.org",
        verbose: true,
    }, (error, mqtt_client) => {
        if (error) {
            return console.log("#", _.error.message(error));
        }
    })

Then we create a MQTT Transporter using the client.

    const mqtt_transporter = mqtt_transport.make({
        prefix: "things",
    }, mqtt_client);

Then we tell the MQTT Transporter to get all the data from the IOTDB Transporter.

    mqtt_transporter.use(iotdb_transporter)

That's it - we are operational. If you go to [mqtt://mqtt.iotdb.org/things](mqtt://mqtt.iotdb.org/things)
you will see all changes to your things being broadcasted.

## Receiving

If you'd like to be able to control IOTDB from MQTT - we don't recommend this
because there isn't a good security model here yet - just add this

    iotdb_transporter.use(mqtt_transporter)


