package limero.mqtt.mqtt2graphite;

import java.util.logging.Logger;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

public class EB {
	static Vertx vertx = Vertx.factory.vertx();
	static EventBus eb = vertx.eventBus();
	static Logger log = Logger.getLogger(EB.class.getName());
	JsonObject json;

	public static EventBus bus() {
		return eb;
	}
	
	public static Vertx vertx(){
		return vertx;
	}

	public static void main(String[] args) {
		
	}

}