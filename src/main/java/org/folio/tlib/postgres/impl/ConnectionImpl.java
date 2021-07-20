package org.folio.tlib.postgres.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public class ConnectionImpl implements SqlConnection {

  final SqlConnection parent;

  ConnectionImpl(SqlConnection parent) {
    this.parent = parent;
  }

  @Override
  public SqlConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
    parent.prepare(s, handler);
    return this;
  }

  @Override
  public Future<PreparedStatement> prepare(String s) {
    return parent.prepare(s);
  }

  @Override
  public SqlConnection exceptionHandler(Handler<Throwable> handler) {
    parent.exceptionHandler(handler);
    return this;
  }

  @Override
  public SqlConnection closeHandler(Handler<Void> handler) {
    parent.closeHandler(handler);
    return this;
  }

  @Override
  public void begin(Handler<AsyncResult<Transaction>> handler) {
    parent.begin(handler);
  }

  @Override
  public Future<Transaction> begin() {
    return parent.begin();
  }

  @Override
  public boolean isSSL() {
    return parent.isSSL();
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return parent.query(s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return parent.preparedQuery(s);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    parent.close(handler);
  }

  @Override
  public Future<Void> close() {
    return parent.close();
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return parent.databaseMetadata();
  }
}
