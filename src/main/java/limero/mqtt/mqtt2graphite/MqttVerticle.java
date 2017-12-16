package limero.mqtt.mqtt2graphite;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;

public class MqttVerticle extends AbstractVerticle {
	MqttClient client;
	EventBus eb;
	static Logger log = LogManager.getLogger(MqttVerticle.class.getSimpleName());
	String _host;
	int _port;
	// Called when verticle is deployed

	long _messagesReceived = 0;
	long _lastMessagesReceivedTime;
	long _lastMessagesReceivedSample;
	
	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);
	}

	public Double getNumber(String str) {
		double d;
		try {
			d = Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return null;
		}
		return d;
	}

	void MqttConnect() {
		_host = config().getString("host", "limero.ddns.net");
		_port = config().getInteger("port",1883);
		log.info(" Mqtt connecting to " + _host + ":" + _port + ".");

		MqttClientOptions options = new MqttClientOptions();
		options.setMaxMessageSize(1000000);
		client = MqttClient.create(vertx, options);
		
		client.connect(_port, _host, s -> {
			if (s.succeeded()) {
				log.info("connection succeeded.");
				client.exceptionHandler(res -> {
					log.error("MQTT exception occured ", res);
				});
				client.closeHandler(res -> {
					log.error(" connection closed ");
					MqttConnect();
				});
				client.publishHandler(msg -> {
					_messagesReceived++;
					log.debug(" => " + msg.topicName() + ":" + msg.payload().toString());
					Double d = getNumber(msg.payload().toString()); 
					if (d != null) {
						String str = msg.topicName();
						String metric = str.replaceAll("/", ".");
						JsonObject json = new JsonObject();
						json.put("time", System.currentTimeMillis() / 1000);
						json.put("metric", metric);
						json.put("value", d);
						eb.publish("graphite", json);
					}

				});
				client.subscribe("#", 1);
			} else {
				log.error(" connection failed ", s.cause());
				client.disconnect();
			}
		});
	}

	void reportMetrics() {
		_lastMessagesReceivedSample = System.currentTimeMillis();
		_messagesReceived = 0;
		long timerID = vertx.setPeriodic(3000, id -> {
			long now = System.currentTimeMillis();
			double timeDelta = now - _lastMessagesReceivedTime;
			double messagesDelta = (_messagesReceived - _lastMessagesReceivedSample) * 1000 / timeDelta;
			log.info(" MQTT messages received : " + _messagesReceived + " " +String.format("%7.2f" , messagesDelta) + " msg/sec ");
			_lastMessagesReceivedSample = _messagesReceived;
			_lastMessagesReceivedTime = now;
		});
	}

	public void start() {

		eb = vertx.eventBus();

		MqttConnect();
		reportMetrics();

	}

	// Optional - called when verticle is undeployed
	public void stop() {
	}
}
