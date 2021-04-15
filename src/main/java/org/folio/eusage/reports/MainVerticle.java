package org.folio.eusage.reports;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger("X");

  @Override
  public void start(Promise<Void> promise) {
    final Router router = Router.router(vertx);

    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-eusage-reports");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8081", config()));
    log.info("Port {}", port);

    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    Future<Void> future = vertx.createHttpServer(so)
        .requestHandler(router)
        .listen(port).mapEmpty();

    future.onComplete(promise::handle);
  }
}
