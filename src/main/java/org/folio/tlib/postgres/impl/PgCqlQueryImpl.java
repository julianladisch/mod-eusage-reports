package org.folio.tlib.postgres.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;
import org.z3950.zing.cql.CQLBooleanNode;
import org.z3950.zing.cql.CQLNode;
import org.z3950.zing.cql.CQLParseException;
import org.z3950.zing.cql.CQLParser;
import org.z3950.zing.cql.CQLPrefixNode;
import org.z3950.zing.cql.CQLSortNode;
import org.z3950.zing.cql.CQLTermNode;

public class PgCqlQueryImpl implements PgCqlQuery {
  private static final Logger log = LogManager.getLogger(PgCqlQueryImpl.class);

  final CQLParser parser = new CQLParser(CQLParser.V1POINT2);
  final Map<String, PgCqlField> fields = new HashMap<>();

  String language = "english";
  CQLNode cqlNodeRoot;

  @Override
  public void parse(String query) {
    if (query == null) {
      cqlNodeRoot = null;
    } else {
      try {
        log.debug("Parsing {}", query);
        cqlNodeRoot = parser.parse(query);
      } catch (CQLParseException | IOException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }

  @Override
  public String getWhereClause() {
    return handleWhere(cqlNodeRoot);
  }

  static String basicOp(CQLTermNode termNode) {
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "==":
        return "=";
      case "=":
      case "<>":
        return base;
      default:
        throw new IllegalArgumentException("Unsupported operator " + base + " for: "
            + termNode.toCQL());
    }
  }

  static String numberOp(CQLTermNode termNode) {
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "==":
        return "=";
      case "=":
      case "<>":
      case ">":
      case "<":
      case "<=":
      case ">=":
        return base;
      default:
        throw new IllegalArgumentException("Unsupported operator " + base + " for: "
            + termNode.toCQL());
    }
  }

  /**
   * See if this is a CQL query with a existence check (NULL or NOT NULL).
   * <p>Empty term makes "IS NULL" for CQL relation =, "IS NOT NULL" for CQL relation <>.
   * </p>
   * @param field CQL field.
   * @param termNode term.
   * @return SQL op for NULL; null if not a NULL check.
   */
  String handleNull(PgCqlField field, CQLTermNode termNode) {
    if (!termNode.getTerm().isEmpty()) {
      return null;
    }
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "=":
        return field.getColumn() + " IS NULL";
      case "<>":
        return field.getColumn() + " IS NOT NULL";
      default:
        return null;
    }
  }

  String handleTypeUuid(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    UUID id = UUID.fromString(termNode.getTerm()); // so IllegalArgumentException is thrown
    String pgTerm = "'" + id + "'";
    String op = basicOp(termNode);
    return field.getColumn() + op + pgTerm;
  }

  static String cqlTermToPgTerm(CQLTermNode termNode, boolean fullText) {
    String cqlTerm = termNode.getTerm();
    StringBuilder pgTerm = new StringBuilder();
    boolean backslash = false;
    for (int i = 0; i < cqlTerm.length(); i++) {
      char c = cqlTerm.charAt(i);
      if (c == '\'') {
        pgTerm.append('\''); // important to avoid SQL injection
      } else if (c == '*' && fullText) {
        if (!backslash) {
          throw new IllegalArgumentException("Masking op * unsupported for: " + termNode.toCQL());
        }
      } else if (c == '?' && fullText) {
        if (!backslash) {
          throw new IllegalArgumentException("Masking op ? unsupported for: " + termNode.toCQL());
        }
      } else if (c == '^' && fullText) {
        if (!backslash) {
          throw new IllegalArgumentException("Anchor op ^ unsupported for: " + termNode.toCQL());
        }
      } else if (backslash) {
        pgTerm.append('\\'); // pass-tru the backslash for Postgres to honor (including \')
      }
      if (c == '\\') {
        backslash = true;
      } else {
        backslash = false;
        pgTerm.append(c);
      }
    }
    if (backslash) {
      pgTerm.append('\\');
    }
    return pgTerm.toString();
  }

  String handleTypeText(PgCqlField field, CQLTermNode termNode, boolean fullText) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String base = termNode.getRelation().getBase();
    if (fullText) {
      fullText = "=".equals(base) || "all".equals(base);
    }
    String pgTerm = cqlTermToPgTerm(termNode, fullText);
    if (fullText) {
      return "to_tsvector('" + language + "', " + field.getColumn() + ") @@ plainto_tsquery('"
          + language + "', E'" + pgTerm + "')";
    }
    return field.getColumn() + " " + basicOp(termNode) + " E'" + pgTerm + "'";
  }

  String handleTypeNumber(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String cqlTerm = termNode.getTerm();
    for (int i = 0; i < cqlTerm.length(); i++) {
      char c = cqlTerm.charAt(i);
      switch (c) {
        case '.':
        case ',':
        case '+':
        case '-':
          break;
        default:
          if (!Character.isDigit(c)) {
            throw new IllegalArgumentException("Bad numeric for: " + termNode.toCQL());
          }
      }
    }
    return field.getColumn() + numberOp(termNode) + cqlTerm;
  }

  String handleTypeBoolean(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String cqlTerm = termNode.getTerm();
    String pgTerm;
    if ("false".equalsIgnoreCase(cqlTerm)) {
      pgTerm = "FALSE";
    } else if ("true".equalsIgnoreCase(cqlTerm)) {
      pgTerm = "TRUE";
    } else {
      throw new IllegalArgumentException("Bad boolean for: " + termNode.toCQL());
    }
    return field.getColumn() + basicOp(termNode) + pgTerm;
  }

  String handleWhere(CQLNode node) {
    if (node == null) {
      return null;
    }
    if (node instanceof CQLBooleanNode) {
      CQLBooleanNode booleanNode = (CQLBooleanNode) node;
      String left = handleWhere(booleanNode.getLeftOperand());
      String right = handleWhere(booleanNode.getRightOperand());
      switch (booleanNode.getOperator()) {
        case OR:
          if (right != null && left != null) {
            return "(" + left + " OR " + right + ")";
          }
          return null;
        case AND:
          if (right != null && left != null) {
            return "(" + left + " AND " + right + ")";
          } else if (right != null) {
            return right;
          } else {
            return left;
          }
        case NOT:
          if (right != null && left != null) {
            return "(" + left + " AND NOT " + right + ")";
          } else if (right != null) {
            return "NOT (" + right + ")";
          }
          return "FALSE";
        default:
          throw new IllegalArgumentException("Unsupported operator "
              + booleanNode.getOperator().name());
      }
    } else if (node instanceof CQLTermNode) {
      CQLTermNode termNode = (CQLTermNode) node;
      PgCqlField field = fields.get(termNode.getIndex().toLowerCase());
      if (field == null) {
        throw new IllegalArgumentException("Unsupported CQL index: " + termNode.getIndex());
      }
      switch (field.getType()) {
        case ALWAYS_MATCHES:
          return null;
        case UUID:
          return handleTypeUuid(field, termNode);
        case TEXT:
          return handleTypeText(field, termNode, false);
        case FULLTEXT:
          return handleTypeText(field, termNode, true);
        case NUMBER:
          return handleTypeNumber(field, termNode);
        case BOOLEAN:
          return handleTypeBoolean(field, termNode);
        default:
          throw new IllegalArgumentException("Unsupported field type: " + field.getType().name());
      }
    } else if (node instanceof CQLSortNode) {
      CQLSortNode sortNode = (CQLSortNode) node;
      throw new IllegalArgumentException("Sorting unsupported: " + sortNode.toCQL());
    } else if (node instanceof CQLPrefixNode) {
      CQLPrefixNode prefixNode = (CQLPrefixNode) node;
      return handleWhere(prefixNode.getSubtree());
    }
    // other node types unsupported, for example proximity
    throw new IllegalArgumentException("Unsupported CQL construct: " + node.toCQL());
  }

  @Override
  public void addField(PgCqlField field) {
    fields.put(field.getName().toLowerCase(), field);
  }
}
