package org.folio.tenant.rest.impl;

import io.swagger.annotations.Api;
import io.vertx.core.Future;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.tenant.main.TenantAttributes;
import org.folio.tenant.main.TenantJob;
import org.folio.tenant.rest.ApiResponse;
import org.folio.tenant.rest.resource.DefaultApi;

// Implement this class

public class TenantApiImpl implements DefaultApi {

  @Override
  public Future<ApiResponse<Void>> deleteTenantJob(String id,
                                                   String okapiTenant, String okapiToken) {
    return Future.succeededFuture(new ApiResponse<>(204));
  }

  @Override
  public Future<ApiResponse<TenantJob>> getTenantJob(String id, String okapiTenant,
                                                     String okapiToken, Integer wait) {
    TenantJob tenantJob = new TenantJob(id, "testlib", null,
        false, null, null);
    return Future.succeededFuture(new ApiResponse<>(200, tenantJob));
  }

  @Override
  public Future<ApiResponse<TenantJob>> postTenant(TenantAttributes tenantAttributes,
                                                   String okapiTenant, String okapiToken) {
    TenantJob tenantJob = new TenantJob("1234", "testlib", null,
        false, null, null);
    return Future.succeededFuture(new ApiResponse<>(201, tenantJob));
  }
}
