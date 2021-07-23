package org.folio.eusage.reports.api;

import static org.folio.eusage.reports.api.EusageReportsApi.agreementEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.packageEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleDataTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleEntriesTable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.TenantPgPoolContainer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.testcontainers.containers.PostgreSQLContainer;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EusageReportsApiTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @ClassRule
  public static RunTestOnContext runTestOnContext = new RunTestOnContext();

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  private static Vertx vertx;
  private static TenantPgPool pool;
  private static String tenant = "tenant";

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = runTestOnContext.vertx();
    TenantPgPool.setModule("mod-eusage-reports-api-test");
    pool = TenantPgPool.pool(vertx, tenant);
    pool.execute(List.of(
        "DROP SCHEMA IF EXISTS {schema} CASCADE",
        "DROP ROLE IF EXISTS {schema}",
        "CREATE ROLE {schema} PASSWORD 'tenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN",
        "GRANT {schema} TO CURRENT_USER",
        "CREATE SCHEMA {schema} AUTHORIZATION {schema}"
    )).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    UUID agreementId = UUID.randomUUID();

    api.populateAgreementLine( null, null, new JsonObject(), agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertTrue(x.getMessage().contains("Failed to decode agreement line:"), x.getMessage())));
  }

  private Future<String> getUseOverTime(String format, String startDate, String endDate) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(ctx.request().params().get("format")).thenReturn(format);
    when(ctx.request().params().get("agreementId")).thenReturn(UUID.randomUUID().toString());
    when(ctx.request().params().get("startDate")).thenReturn(startDate);
    when(ctx.request().params().get("endDate")).thenReturn(endDate);
    return new EusageReportsApi().getUseOverTime(vertx, ctx)
    .map(x -> {
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(ctx.response()).end(argument.capture());
      return argument.getValue();
    });
  }

  @Test
  public void useOverTimeStartDateAfterEndDate() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
        getUseOverTime("JOURNAL", "2020-04-01", "2020-02-01"));
    assertThat(t.getMessage(), is("startDate=2020-04-01 is after endDate=2020-02-01"));
  }

  @Test
  public void useOverTimeJournal(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    api.postInit(vertx, tenant, new JsonObject().put("module_to", "1.1.1"))
    .compose(x -> getUseOverTime("JOURNAL", "2020-03-01", "2020-05-01"))
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json, containsString("2020-04"));
    }));
  }

  @Test
  public void useOverTimeBook() {
    assertThat(getUseOverTime("BOOK", "2020-03-01", "2020-04-01").result(), containsString("2020-03"));
  }

  @Test
  public void useOverTimeDatabase() {
    assertThat(getUseOverTime("DATABASE", "2020-03-01", "2020-04-01").result(), containsString("2020-03"));
  }

  @Test
  public void useOverTimeUnknownFormat() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("FOO", "2020-04-01", "2020-02-01"));
    assertThat(t.getMessage(), containsString("format = FOO"));
  }

  // agreementId
  String a1  = "10000000-0000-4000-8000-000000000000";
  String a2  = "20000000-0000-4000-8000-000000000000";
  // kbTitleId
  String t11 = "11000000-0000-4000-8000-000000000000";
  String t12 = "12000000-0000-4000-8000-000000000000";
  String t21 = "21000000-0000-4000-8000-000000000000";
  String t22 = "22000000-0000-4000-8000-000000000000";
  // kbPackageId
  String p11 = "1100000a-0000-4000-8000-000000000000";
  String p12 = "1200000a-0000-4000-8000-000000000000";
  String p21 = "2100000a-0000-4000-8000-000000000000";
  String p22 = "2200000a-0000-4000-8000-000000000000";
  // titleEntryId
  String te11 = "1100000e-0000-4000-8000-000000000000";
  String te12 = "1200000e-0000-4000-8000-000000000000";
  String te21 = "2100000e-0000-4000-8000-000000000000";
  String te22 = "2200000e-0000-4000-8000-000000000000";

  private Future<RowSet<Row>> insertAgreement(String agreementId, String titleId, String packageId) {
    return pool.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
        + "(id, agreementId, kbTitleId, kbPackageId) VALUES ($1, $2, $3, $4)")
    .execute(Tuple.of(UUID.randomUUID(), agreementId, titleId, packageId));
  }

  private Future<RowSet<Row>> insertPackageEntry(String packageId, String packageName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + packageEntriesTable(pool)
        + "(kbPackageId, kbPackageName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(packageId, packageName, titleId));
  }

  private Future<RowSet<Row>> insertTitleEntry(String titleEntryId,
      String titleName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
        + "(id, kbTitleName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(titleEntryId, titleName, titleId));
  }

  private Future<RowSet<Row>> insertTitleData(String titleEntryId,
      String dateStart, String dateEnd, boolean openAccess, int uniqueAccessCount, int totalAccessCount) {
    return pool.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, titleEntryId, usageDateRange, openAccess, uniqueAccessCount, totalAccessCount) "
        + "VALUES ($1, $2, daterange($3::text::date, $4::text::date), $5, $6, $7)")
    .execute(Tuple.of(UUID.randomUUID(), titleEntryId, dateStart, dateEnd, openAccess, uniqueAccessCount, totalAccessCount));
  }

  @Test
  public void getUseOverTimeJournal(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    api.postInit(vertx, tenant, new JsonObject().put("module_to", "1.1.1"))
    .compose(x -> insertAgreement(a1, t11, p11))
    .compose(x -> insertAgreement(a1, t12, p12))
    .compose(x -> insertAgreement(a2, t21, p21))
    .compose(x -> insertAgreement(a2, t22, p22))
    .compose(x -> insertPackageEntry(p11, "Package 11", t11))
    .compose(x -> insertPackageEntry(p12, "Package 12", t21))
    .compose(x -> insertPackageEntry(p21, "Package 21", t12))
    .compose(x -> insertPackageEntry(p22, "Package 22", t22))
    .compose(x -> insertTitleEntry(te11, "Title 11", t11))
    .compose(x -> insertTitleEntry(te12, "Title 12", t12))
    .compose(x -> insertTitleEntry(te21, "Title 21", t21))
    .compose(x -> insertTitleEntry(te22, "Title 22", t22))
    .compose(x -> insertTitleData(te11, "2020-03-01", "2020-04-01", false, 1, 2))
    .compose(x -> insertTitleData(te11, "2020-04-01", "2020-04-15", false, 2, 3))
    .compose(x -> insertTitleData(te11, "2020-04-15", "2020-05-01", false, 3, 3))
    .compose(x -> insertTitleData(te11, "2020-05-01", "2020-06-01", false, 4, 12))
    .compose(x -> insertTitleData(te12, "2020-03-01", "2020-04-01", false, 11, 12))
    .compose(x -> insertTitleData(te12, "2020-04-01", "2020-05-01", false, 15, 16))
    .compose(x -> insertTitleData(te12, "2020-05-01", "2020-06-01", false, 14, 22))
    .compose(x -> insertTitleData(te21, "2020-03-01", "2020-04-01", false, 0, 0))
    .compose(x -> insertTitleData(te21, "2020-05-01", "2020-06-01", false, 20, 40))
    .compose(x -> insertTitleData(te21, "2020-06-01", "2020-07-01", true, 1, 2))

    .compose(x -> api.getUseOverTimeJournal(pool, a1, "2020-04-01", "2020-06-01"))
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getString("agreementId"), is(a1));
      assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
      assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(22L, 34L)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(20L, 18L)));
      assertThat(json.getJsonArray("items").size(), is(8));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", null)
              .put("onlineISSN", null)
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 18)
              .put("accessCountsByPeriod", new JsonArray("[ 6, 12 ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", null)
              .put("onlineISSN", null)
              .put("accessType", "Controlled")
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", 9)
              .put("accessCountsByPeriod", new JsonArray("[ 5, 4 ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", null)
              .put("onlineISSN", null)
              .put("accessType", "OA_Gold")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", null)
              .put("accessCountsByPeriod", new JsonArray("[ null, null ]"))
              .encodePrettily()));
    }))
    // openAccess
    .compose(x -> api.getUseOverTimeJournal(pool, a2, "2020-06-01", "2020-07-01"))
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(2L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(1L));
      JsonObject item2 = json.getJsonArray("items").getJsonObject(2);
      JsonObject item3 = json.getJsonArray("items").getJsonObject(3);
      assertThat(item2.getString("accessType"), is("OA_Gold"));
      assertThat(item3.getString("accessType"), is("OA_Gold"));
      assertThat(item2.getString("metricType"), is("Total_Item_Requests"));
      assertThat(item2.getLong("accessCountTotal"), is(2L));
      assertThat(item3.getString("metricType"), is("Unique_Item_Requests"));
      assertThat(item3.getLong("accessCountTotal"), is(1L));
    }))
    // time without any data, totals should be null
    .compose(x -> api.getUseOverTimeJournal(pool, a2, "1999-12-01", "2000-02-01"))
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(nullValue()));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(nullValue()));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining((Long)null, null)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining((Long)null, null)));
    }));
  }
}
