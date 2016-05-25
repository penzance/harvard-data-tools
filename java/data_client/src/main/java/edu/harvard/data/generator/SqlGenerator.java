package edu.harvard.data.generator;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.existing.ExistingSchemaTable;

public class SqlGenerator {
  public static String generateCreateStatement(final DataSchemaTable table) {
    String s = "CREATE TABLE " + table.getTableName() + " (\n";
    final List<DataSchemaColumn> columns = table.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      String columnName = column.getName();
      if (columnName.contains(".")) {
        columnName = columnName.substring(columnName.lastIndexOf(".") + 1);
      }
      final String redshiftType = column.getType().getRedshiftType(column.getLength());
      s += "    " + columnName + " " + redshiftType;
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

  public static String generateUnloadStatement(final ExistingSchemaTable table,
      final DataSchemaTable tableSchema, final String s3Location, final String awsKey,
      final String awsSecret, final Date dataBeginDate) {
    String s = "UNLOAD ('SELECT ";
    final List<DataSchemaColumn> columns = tableSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      s += "\"" + columns.get(i).getName() + "\"";
      if (i < columns.size() - 1) {
        s += ", ";
      }
    }
    s += " FROM " + table.getSourceTable();

    if (table.getDays() != null) {
      final Calendar fromDate = new GregorianCalendar();
      fromDate.setTime(dataBeginDate);
      fromDate.set(Calendar.HOUR_OF_DAY, 0);
      fromDate.set(Calendar.MINUTE, 0);
      fromDate.add(Calendar.DATE, (table.getDays() * -1));

      final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      s += " WHERE " + table.getTimestampColumn() + " >= \\'" + format.format(fromDate.getTime())
      + "\\'";
    }

    s += "') TO '" + s3Location + "/" + table.getTableName() + "/' CREDENTIALS 'aws_access_key_id=" + awsKey
        + ";aws_secret_access_key=" + awsSecret + "' delimiter '\\t' ALLOWOVERWRITE;";

    return s;
  }

}
