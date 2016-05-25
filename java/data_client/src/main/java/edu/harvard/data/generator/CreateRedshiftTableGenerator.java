package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.harvard.data.schema.DataSchemaTable;

public class CreateRedshiftTableGenerator {

  private final File dir;
  private final GenerationSpec schemaVersions;

  public CreateRedshiftTableGenerator(final File dir, final GenerationSpec schemaVersions) {
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
    final List<String> tableNames = new ArrayList<String>();
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      tableNames.add(table.getTableName());
    }
    Collections.sort(tableNames);
    for (final String tableName : tableNames) {
      final DataSchemaTable table = phase.getSchema().getTableByName(tableName);
      if (!table.isTemporary()) {
        out.println(SqlGenerator.generateCreateStatement(table));
      }
    }
  }

}
