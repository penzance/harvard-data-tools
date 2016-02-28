package edu.harvard.data.client.generator.redshift;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import edu.harvard.data.client.generator.SchemaPhase;
import edu.harvard.data.client.generator.SchemaTransformer;
import edu.harvard.data.client.schema.DataSchemaTable;

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
      out.println(SqlGenerator.generateCreateStatement(table));
    }
  }

}