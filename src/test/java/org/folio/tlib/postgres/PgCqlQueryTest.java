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

    pgCqlQuery.addField(new PgCqlField("dc.title", "title", PgCqlField.Type.TEXT));

    pgCqlQuery.parse("dc.Title==value");
    Assert.assertEquals("title = E'value'", pgCqlQuery.getWhereClause());
  }

  static String ftResponse(String column, String term) {
    return "to_tsvector('english', " + column + ") @@ plainto_tsquery('english', E'" + term + "')";

  }
  @Test
  public void testQueries() {
    String[][] list = new String[][] {
        { "(", "error: expected index or term, got EOF" },
        { "foo=bar", "error: Unsupported CQL index: foo" },
        { "Title=v1", ftResponse("title", "v1") },
        { "Title all v1", ftResponse("title", "v1") },
        { "Title>v1", "error: Unsupported operator > for: Title > v1" },
        { "Title=\"men's room\"", ftResponse("title", "men''s room") },
        { "Title=men's room", ftResponse("title", "men''s room") },
        { "Title=v1*", "error: Masking op * unsupported for: Title = v1*" },
        { "Title=v1?", "error: Masking op ? unsupported for: Title = v1?" },
        { "Title=v1^", "error: Anchor op ^ unsupported for: Title = v1^" },
        { "Title=a\\*b", ftResponse("title", "a*b") },
        { "Title=a\\^b", ftResponse("title", "a^b") },
        { "Title=a\\?b", ftResponse("title", "a?b") },
        { "Title=a\\?b", ftResponse("title", "a?b") },
        { "Title=a\\n", ftResponse("title", "a\\n") },
        { "Title=\"a\\\"\"", ftResponse("title", "a\"") },
        { "Title=\"a\\\"b\"", ftResponse("title", "a\"b") },
        { "Title=a\\12", ftResponse("title", "a\\12") },
        { "Title=a\\\\", ftResponse("title", "a\\\\") },
        { "Title=a\\'", ftResponse("title", "a\\'") },
        { "Title=a\\'b", ftResponse("title", "a\\'b") },
        { "Title=a\\\\\\n", ftResponse("title", "a\\\\\\n") },
        { "Title=a\\\\\\?", ftResponse("title", "a\\\\?") },
        { "Title=\"\"", "title IS NULL" },
        { "Title<>\"\"", "title IS NOT NULL" },
        { "Title==\"\"", "title = E''" },
        { "Title>\"\"", "error: Unsupported operator > for: Title > \"\"" },
        { "Title==v1 or title==v2",  "(title = E'v1' OR title = E'v2')"},
        { "cql.allRecords=1 or title==v1", null },
        { "title==v1 or cql.allRecords=1", null },
        { "Title==v1 and title==v2", "(title = E'v1' AND title = E'v2')" },
        { "Title==v1 and cql.allRecords=1", "title = E'v1'" },
        { "cql.allRecords=1 and Title==v2", "title = E'v2'" },
        { "Title==v1 not title==v2", "(title = E'v1' AND NOT title = E'v2')" },
        { "cql.allRecords=1 not title==v2", "NOT (title = E'v2')" },
        { "title==v1 not cql.allRecords=1", "FALSE" },
        { "title==v1 prox title==v2", "error: Unsupported operator PROX" },
        { "cost=1", "cost=1" },
        { "cost=+1.9", "cost=+1.9" },
        { "cost=-1,90", "cost=-1,90" },
        { "cost=0x100", "error: Bad numeric for: cost = 0x100" },
        { "cost>1", "cost>1" },
        { "cost>=2", "cost>=2" },
        { "cost==3", "cost=3" },
        { "cost<>4", "cost<>4" },
        { "cost<5", "cost<5" },
        { "cost<=6", "cost<=6" },
        { "cost adj 7", "error: Unsupported operator adj for: cost adj 7" },
        { "cost=\"\"", "cost IS NULL" },
        { "paid=true", "paid=TRUE" },
        { "paid=False", "paid=FALSE" },
        { "paid=fals", "error: Bad boolean for: paid = fals" },
        { "paid=\"\"", "paid IS NULL" },
        { "id=null", "error: Invalid UUID string: null" },
        { "id=\"\"", "id IS NULL" },
        { "id=6736bd11-5073-4026-81b5-b70b24179e02", "id='6736bd11-5073-4026-81b5-b70b24179e02'" },
        { "id<>6736bd11-5073-4026-81b5-b70b24179e02", "id<>'6736bd11-5073-4026-81b5-b70b24179e02'" },
        { "title==v1 sortby cost", "title = E'v1'"},
        { ">x = \"http://foo.org/p\" title==v1 sortby cost", "title = E'v1'"},
    };
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("title", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("cost", PgCqlField.Type.NUMBER));
    pgCqlQuery.addField(new PgCqlField("paid", PgCqlField.Type.BOOLEAN));
    pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
    for (String [] entry : list) {
      String query = entry[0];
      String expect = entry[1];
      try {
        pgCqlQuery.parse(query);
        Assert.assertEquals("CQL: " + query, expect, pgCqlQuery.getWhereClause());
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(expect, "error: " + e.getMessage());
      }
    }
  }

}
