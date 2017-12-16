package limero.mqtt.mqtt2graphite;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

public class GraphiteVerticle extends AbstractVerticle {
	EventBus eb;
	NetClientOptions options = new NetClientOptions().setConnectTimeout(10000);
	NetClient client;
	NetSocket socket;
	static Logger log = LogManager.getLogger(GraphiteVerticle.class.getSimpleName());

	long _messagesReceived = 0;
	long _lastMessagesReceivedTime;
	long _lastMessagesReceivedSample;
/* ==================================================  CONFIG */	
	String _metricsPrefix;
	int _metricsInterval;
	String _host;
	int _port;
	String _subscribeTo;
	String _publishMetricsTo;
/* ==========================================================*/
	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);
	}
	
	void SocketConnect() {
		_host = config().getString("host", "limero.ddns.net");
		_port = config().getInteger("port", 1883);
		_publishMetricsTo = config().getString("publishMetricsTo", "metrics");
		_subscribeTo = config().getString("subscribeTo", "metrics");
		
		client = vertx.createNetClient(options);
		client.connect(_port, _host, res -> {
			if (res.succeeded()) {
				System.out.println("Connected!");
				socket = res.result();
				socket.closeHandler(r -> {
					SocketConnect();
				});
			} else {
				log.error("Failed to connect: " + res.cause().getMessage());
			}
		});
	}
	
	void reportMetric(String name,double value,long time){
		JsonObject json = new JsonObject();
		json.put("time", time);
		json.put("metric", name);
		json.put("value", value);
		eb.publish(_publishMetricsTo, json);
	}

	void reportMetrics() {
		_lastMessagesReceivedSample = System.currentTimeMillis();
		_messagesReceived = 0;
		vertx.setPeriodic(3000, id -> {
			long now = System.currentTimeMillis();
			double timeDelta = (now - _lastMessagesReceivedTime)/1000.0;
			double messagesDelta = (_messagesReceived - _lastMessagesReceivedSample) / timeDelta;
			
			reportMetric(_metricsPrefix+"messagesSend",_messagesReceived,System.currentTimeMillis()/1000);

			
			log.info(" Graphite messages send : " + _messagesReceived + " " + String.format("%7.2f", messagesDelta)
					+ " msg/sec ");
			_lastMessagesReceivedSample = _messagesReceived;
			_lastMessagesReceivedTime = now;
		});
	}

	// Called when verticle is deployed
	public void start() {
		
		_host = config().getString("host", "limero.ddns.net");
		_port = config().getInteger("port",1883);
		_metricsPrefix = config().getString("metricsPrefix", "src/mqtt2graphite/");
		_metricsInterval = config().getInteger("metricsInterval",5000);
		_subscribeTo =config().getString("subscribeTo","metrics");
		
		eb = vertx.eventBus();
		SocketConnect();

		eb.consumer(_subscribeTo, message -> {
			log.debug("I have received a message: " + message.body());
			JsonObject json = (JsonObject) message.body();
			if (socket != null) {
				String line = json.getString("metric") + " " + json.getDouble("value") + " " + json.getInteger("time");
				log.debug(line);
				socket.write(line + "\n");
				_messagesReceived++;
			}
		});
		reportMetrics();
	}

	// Optional - called when verticle is undeployed
	public void stop() {
	}

}
