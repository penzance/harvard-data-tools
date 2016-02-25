package edu.harvard.data.client.generator.redshift;

import java.util.List;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;

public class SqlGenerator {
  public static String generateCreateStatement(final DataSchemaTable table) {
    String s = "CREATE TABLE " + table.getTableName() + " (";
    final List<DataSchemaColumn> columns = table.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      final String redshiftType = column.getType().getRedshiftType(column.getLength());
      s += "    " + column.getName() + " " + redshiftType;
      if (i < columns.size() - 1) {
        s += ",\n";
      } else {
        s += "\n";
      }
    }
    s += ");\n";
    return s;
  }

  public static String generateAlterStatement(final DataSchemaTable table,
      final DataSchemaColumn column) {
    final String redshiftType = column.getType().getRedshiftType(column.getLength());
    return "ALTER TABLE " + table.getTableName() + " ADD COLUMN " + column.getName() + " "
    + redshiftType + ";";
  }
}
