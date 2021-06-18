package org.folio.eusage.reports.api;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EusageReportsApiTest {
  private final static Logger log = LogManager.getLogger("xx");

  static Vertx vertx;

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    UUID agreementId = UUID.randomUUID();

    api.populateAgreementLine(new JsonObject(), null, agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertEquals("Missing property resource from agreement " + agreementId, x.getMessage())));
    api.populateAgreementLine(new JsonObject().put("resource", new JsonObject()), null, agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertEquals("Missing property _object from agreement " + agreementId, x.getMessage())));
    api.populateAgreementLine(new JsonObject().put("resource",new JsonObject().put("_object", new JsonObject())), null, agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertEquals("Missing property pti from agreement " + agreementId, x.getMessage())));
    api.populateAgreementLine(new JsonObject().put("resource",new JsonObject().put("_object", new JsonObject().put("pti", new JsonObject()))), null, agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertEquals("Missing property titleInstance from agreement " + agreementId, x.getMessage())));
  }

}
