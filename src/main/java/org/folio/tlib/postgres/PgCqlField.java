package org.folio.tlib.postgres;

public class PgCqlField {

  public enum Type {
    ALWAYS_MATCHES, UUID, TEXT, NUMBER, BOOLEAN
  }

  final String name;
  final String column;
  final Type type;

  /**
   * Define CQL field to Pg mapping.
   * @param name name of index in CQL  and Pg column.
   * @param type data type.
   */
  public PgCqlField(String name, Type type) {
    this.name = name;
    this.column = name;
    this.type = type;
  }

  /**
   * Define CQL field to Pg mapping.
   * @param name name of index in CQL.
   * @param column Pg column.
   * @param type data type.
   */
  public PgCqlField(String name, String column, Type type) {
    this.name = name;
    this.column = column;
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getColumn() {
    return name;
  }
}
