package edu.harvard.data.data_tool_generator.redshift;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.data_tool_generator.SchemaPhase;
import edu.harvard.data.data_tool_generator.SchemaTransformer;

public class CreateRedshiftTableGenerator {

  private final File dir;
  private final SchemaTransformer schemaVersions;

  public CreateRedshiftTableGenerator(final File dir, final SchemaTransformer schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    final File createTableFile = new File(dir, "create_redshift_tables.sql");

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateCreateTableFile(out, schemaVersions.getPhase(2));
    }
  }

  private void generateCreateTableFile(final PrintStream out, final SchemaPhase phase) {
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      out.println("CREATE TABLE " + table.getTableName() + "(");
      final List<DataSchemaColumn> columns = table.getColumns();
      for (int i=0; i<columns.size(); i++) {
        final DataSchemaColumn column = columns.get(i);
        final String redshiftType = column.getType().getRedshiftType(column.getLength());
        out.print("    " + column.getName() + " " + redshiftType);
        if (i < columns.size() - 1) {
          out.println(",");
        } else {
          out.println();
        }
      }
      out.println(");");
    }
  }

}
