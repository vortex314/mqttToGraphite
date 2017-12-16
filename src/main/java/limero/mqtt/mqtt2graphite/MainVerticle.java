package limero.mqtt.mqtt2graphite;

import io.vertx.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
	long counter=0; 
	

	@Override
	public void start() throws Exception {
		vertx.createHttpServer().requestHandler(req -> {
			req.response().putHeader("content-type", "text/plain").end("Hello from Vert.x!");
		}).listen(8080);
		System.out.println("HTTP server started on port 8080");
		vertx.deployVerticle(new GraphiteVerticle());
		vertx.deployVerticle(new MqttVerticle());

	}
}
