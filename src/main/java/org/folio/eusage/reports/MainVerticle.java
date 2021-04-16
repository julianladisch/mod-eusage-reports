package org.folio.eusage.reports;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger("MainVerticle");

  @Override
  public void start(Promise<Void> promise) {
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-eusage-reports");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8081", config()));
    log.info("Port {}", port);

    RouterBuilder.create(vertx, "src/main/resources/openapi/tenant2_0.yaml")
        .<Void>compose(routerBuilder -> {
          routerBuilder
              .operation("postTenant")
              .handler(ctx -> {
                log.info("postTenant handler");
                ctx.response().setStatusCode(201);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Created");
              })
              .failureHandler(ctx -> {
                log.info("postTenant failureHandler");
                ctx.response().setStatusCode(400);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Faulure");
              });
          Router router = routerBuilder.createRouter();
          HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
          return vertx.createHttpServer(so)
              .requestHandler(router)
              .listen(port).mapEmpty();
        })
        .onComplete(promise::handle);
  }
}
