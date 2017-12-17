# An MQTT To Graphite TSDB Converter
All metrics belonging to a certain subscribe pattern are evaluated if they are a double number. 
If so they are send to Graphite where the metric name is based on the topic name and slashes are replaced by dots.
The timestamp is the moment the metric is received
It is based on a Vert.x implementation with 2 verticles :
- 1/ An MQTT verticle receiving and filtering the metrics
Sending them on the eventbus to destination "graphite"
- 2/ A graphite verticle listening on the eventbus and sending the results to Graphite.

In this way the eventbus is used as queueing between MQTT and Graphite.
Normally each verticle should handle their own reconnects

The configuration can be adapted by changing the config.json file. 

```
{
	"mqtt": {
		"class": "limero.mqtt.mqtt2graphite.MqttVerticle",
		"worker": true,
		"config": {
			"publishTo": "metrics",
			"host": "limero.ddns.net",
			"port": 1883,
			"subscribe": [
				"src/#",
				"dst/#"
			],
			"metricsInterval": 3000,
			"metricsPrefix": "src/localhost/mqtt2graphite/mqtt/"
		}
	},
	"graphite": {
		"class": "limero.mqtt.mqtt2graphite.GraphiteVerticle",
		"config": {
			"subscribeTo": "metrics",
			"publishMetricsTo": "metrics",
			"host": "192.168.0.149",
			"port": 2003,
			"metricsInterval": 3000,
			"metricsPrefix": "src/localhost/mqtt2graphite/graphite/"
		}
	}
}
```
![Sequence Diagram](https://github.com/vortex314/mqtt2graphite/doc/mqtt2graphite.png)
