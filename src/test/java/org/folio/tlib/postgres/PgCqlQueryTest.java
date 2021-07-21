package org.folio.tlib.postgres;

import org.junit.Assert;
import org.junit.Test;

public class PgCqlQueryTest {

  @Test
  public void testSimple() {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    Assert.assertNull(pgCqlQuery.getWhereClause());
    pgCqlQuery.parse(null);
    Assert.assertNull(pgCqlQuery.getWhereClause());

    pgCqlQuery.addField(new PgCqlField("title", PgCqlField.Type.TEXT));

    pgCqlQuery.parse("Title=value");
    Assert.assertEquals("title='value'", pgCqlQuery.getWhereClause());
  }

  @Test
  public void testBoolean() {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("title", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));

    pgCqlQuery.parse("Title=v1 or title=v2");
    Assert.assertEquals("(title='v1' OR title='v2')", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("cql.allRecords=1 or title=v1");
    Assert.assertNull(pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("title=v1 or cql.allRecords=1");
    Assert.assertNull(pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("Title=v1 and title=v2");
    Assert.assertEquals("(title='v1' AND title='v2')", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("Title=v1 and cql.allRecords=1");
    Assert.assertEquals("title='v1'", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("cql.allRecords=1 and Title=v2");
    Assert.assertEquals("title='v2'", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("Title=v1 not title=v2");
    Assert.assertEquals("(title='v1' AND NOT title='v2')", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("cql.allRecords=1 not title=v2");
    Assert.assertEquals("NOT (title='v2')", pgCqlQuery.getWhereClause());
    pgCqlQuery.parse("title=v1 not cql.allRecords=1");
    Assert.assertEquals("FALSE", pgCqlQuery.getWhereClause());

  }

}
