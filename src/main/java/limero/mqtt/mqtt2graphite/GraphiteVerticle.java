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
	String _host;
	int _port;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);
	}
	
	void SocketConnect() {
		_host = config().getString("host", "limero.ddns.net");
		_port = config().getInteger("port", 1883);
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

	void reportMetrics() {
		_lastMessagesReceivedSample = System.currentTimeMillis();
		_messagesReceived = 0;
		vertx.setPeriodic(3000, id -> {
			long now = System.currentTimeMillis();
			double timeDelta = now - _lastMessagesReceivedTime;
			double messagesDelta = (_messagesReceived - _lastMessagesReceivedSample) * 1000.0 / timeDelta;
			log.info(" Graphite messages send : " + _messagesReceived + " " + String.format("%7.2f", messagesDelta)
					+ " msg/sec ");
			_lastMessagesReceivedSample = _messagesReceived;
			_lastMessagesReceivedTime = now;
		});
	}

	// Called when verticle is deployed
	public void start() {
		eb = vertx.eventBus();
		SocketConnect();

		eb.consumer("graphite", message -> {
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
