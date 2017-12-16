package limero.mqtt.mqtt2graphite;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import org.apache.log4j.Logger;

public class Launcher {
	static Logger log = Logger.getLogger(Launcher.class);

	Handler<AsyncResult<Buffer>> f;

	public static void main(String[] args) {
		/*
		 * Properties props = new Properties();
		 * props.setProperty("java.util.logging.SimpleFormatter.format",
		 * "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n"); //
		 * System.setProperties(props);
		 * System.setProperty("java.util.logging.SimpleFormatter.format",
		 * "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
		 */

		// Handler<AsyncResult<Buffer>>()

		String configFile = "src/main/resources/config.json";
		if (args.length > 0) {
			configFile = args[0];
		}
		Vertx vertx = EB.vertx();

		try {
			vertx.fileSystem().readFile(configFile, new Handler<AsyncResult<Buffer>>() {
				public void handle(AsyncResult<Buffer> users) {
					if (users.succeeded()) {
						JsonObject verticles = new JsonObject(users.result().toString());
						String[] fieldnames = new String[verticles.fieldNames().size()];
						verticles.fieldNames().toArray(fieldnames);

						for (int i = 0; i < fieldnames.length; i++) {
							String field = fieldnames[i];
							String nextField = i < fieldnames.length - 1 ? fieldnames[i + 1] : "";
							JsonObject verticle = verticles.getJsonObject(field);
							log.info(" launching verticle " + field + " : " + verticle.getString("class"));
							try {

								DeploymentOptions options = new DeploymentOptions();
								JsonObject config = verticle.getJsonObject("config");
								config.put("name", field);
								if (!config.containsKey("next"))
									config.put("next", nextField);
								options.setConfig(config);
								options.setWorker(verticle.getBoolean("worker", false));
								options.setWorkerPoolSize(verticle.getInteger("poolSize", 1));
								Verticle v = (Verticle) Class.forName(verticle.getString("class")).newInstance();

								vertx.deployVerticle(v, options);

							} catch (Exception e) {
								log.warn("verticle start failed.", e);
							}
						}
					} else {
						log.warn("config file parsing failed.", users.cause());
					}
				}

			});
		} catch (Exception e) {
			log.warn("file load failed ", e);
		}

	}

}
