package edu.harvard.data.data_tool_generator.redshift;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import edu.harvard.data.client.canvas.api.CanvasDataSchemaColumn;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaTable;
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
    final File createTableFile = new File(dir, "canvas_create_redshift_tables.sql");

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateCreateTableFile(out, schemaVersions.getPhase(2));
    }
  }

  private void generateCreateTableFile(final PrintStream out, final SchemaPhase phase) {
    for (final CanvasDataSchemaTable table : phase.getSchema().getSchema().values()) {
      out.println("CREATE TABLE " + table.getTableName() + "(");
      final List<CanvasDataSchemaColumn> columns = table.getColumns();
      for (int i=0; i<columns.size(); i++) {
        final CanvasDataSchemaColumn column = columns.get(i);
        out.print("    " + column.getName() + " " + column.getRedshiftType());
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
