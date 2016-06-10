package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.schema.DataSchemaTable;

public class CreateRedshiftTableGenerator {

  private final File dir;
  private final GenerationSpec spec;
  private final DataConfig config;

  public CreateRedshiftTableGenerator(final File dir, final GenerationSpec spec,
      final DataConfig config) {
    this.dir = dir;
    this.spec = spec;
    this.config = config;
  }

  public void generate() throws IOException {
    final File createTableFile = new File(dir, "create_redshift_tables.sql");

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateCreateTableFile(out, spec.getPhase(2));
    }
  }

  private void generateCreateTableFile(final PrintStream out, final SchemaPhase phase) {
    out.println("create schema " + config.getDatasetName() + ";");
    out.println();
    final List<String> tableNames = new ArrayList<String>();
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      tableNames.add(table.getTableName());
    }
    Collections.sort(tableNames);
    for (final String tableName : tableNames) {
      final DataSchemaTable table = phase.getSchema().getTableByName(tableName);
      if (!table.isTemporary()) {
        out.println(SqlGenerator.generateCreateStatement(table, config.getDatasetName()));
      }
    }
  }

}
