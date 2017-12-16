An MQTT To Graphite Converter
All metrics belonging to a certain subscribe pattern are evaluated if they are a double number. If so they are send to Graphite where the metric name is based on the topic name and slashes are replaced by dots.
The timestamp is the moemnt the metric is received
It is based on a Vert.x implementation with 2 verticles :
1/ An MQTT verticle receiving and filtering the metrics
Sending them on the eventbus to destination "graphite"
2/ A graphite verticle listening on the eventbus and sending the resulst to Graphite.

In this way the eventbus is used as queueing between MQTT and Graphite.
Normally each verticle hosuld handle their own reconnects

