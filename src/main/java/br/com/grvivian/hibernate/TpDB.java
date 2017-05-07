package br.com.grvivian.hibernate;

/**
 *
 * @author glaucio
 */
public enum TpDB {
  MYSQL("MySQL"),
  POSTGRES("PostgreSQL"),
  FIREBIRD("Firebir");

  private final String label;

  private TpDB(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return this.label;
  }
}
