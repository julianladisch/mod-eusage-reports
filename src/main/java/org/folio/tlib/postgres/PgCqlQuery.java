package org.folio.tlib.postgres;

import org.folio.tlib.postgres.impl.PgCqlQueryImpl;

public interface PgCqlQuery {

  static PgCqlQuery query() {
    return new PgCqlQueryImpl();
  }

  /**
   * Parse CQL query string.
   * <p>Throws IllegalArgumentException on syntax error</p>
   * @param query CQL query string.
   *
   */
  void parse(String query);

  /**
   * Get PostgresQL where clause (without WHERE).
   * <p>Throws IllegalArgumentException on syntax error</p>
   * @return where clause argument or null if "always true" (WHERE can be omitted).
   */
  String getWhereClause();

  /**
   * Add supported field.
   * @param field field.
   */
  void addField(PgCqlField field);
}
