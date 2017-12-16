package limero.mqtt.mqtt2graphite;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;

public class MqttVerticle extends AbstractVerticle {
	MqttClient client;
	EventBus eb;
	static Logger log = LogManager.getLogger(MqttVerticle.class.getSimpleName());
	
//===============================================  CONFIG
	String _host;
	int _port;
	JsonArray _subscribe;
	String _publishTo;
	// Called when verticle is deployed

	long _messagesReceived = 0;
	long _numbersReceived=0;
	
	long _lastMessagesReceivedTime;
	long _lastMessagesReceivedSample;
	String _metricsPrefix;
	int _metricsInterval;
	
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
		_metricsPrefix = config().getString("metricsPrefix", "src/mqtt2graphite/");
		_metricsInterval = config().getInteger("metricsInterval",5000);
		_publishTo = config().getString("publishTo", "metrics");

		List<String> defaultSubscribe=new Vector<String>();
		defaultSubscribe.add("#");
		_subscribe =  config().getJsonArray("subscribe",new JsonArray(defaultSubscribe));
		
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
						_numbersReceived++;
						String str = msg.topicName();
						String metric = str.replaceAll("/", ".");
						reportMetric(metric,d,System.currentTimeMillis() / 1000);
					}

				});
				client.subscribe("#", 1);
			} else {
				log.error(" connection failed ", s.cause());
				client.disconnect();
			}
		});
	}

	void reportMetric(String name,double value,long time){
		JsonObject json = new JsonObject();
		json.put("time", time);
		json.put("metric", name);
		json.put("value", value);
		eb.publish(_publishTo, json);
	}
	
	void reportMetrics() {
		_lastMessagesReceivedSample = System.currentTimeMillis();
		_messagesReceived = 0;
		long timerID = vertx.setPeriodic(_metricsInterval, id -> {
			long now = System.currentTimeMillis();
			double timeDelta = now - _lastMessagesReceivedTime;
			double messagesDelta = (_messagesReceived - _lastMessagesReceivedSample) * 1000 / timeDelta;
			
			reportMetric(_metricsPrefix+"messagesReceived",_messagesReceived,System.currentTimeMillis()/1000);
			reportMetric(_metricsPrefix+"numberReceived",_numbersReceived,System.currentTimeMillis()/1000);
			reportMetric(_metricsPrefix+"messagesPerSec",messagesDelta,System.currentTimeMillis()/1000);
			
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
