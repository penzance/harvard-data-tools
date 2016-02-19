package edu.harvard.data.data_tool_generator.bash;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.data_tool_generator.SchemaPhase;
import edu.harvard.data.data_tool_generator.SchemaTransformer;

public class HDFSCopyUnmodifiedTableGenerator {
  private final File dir;
  private final SchemaTransformer schemaVersions;

  public HDFSCopyUnmodifiedTableGenerator(final File dir, final SchemaTransformer schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    try (final PrintStream out = new PrintStream(
        new FileOutputStream(new File(dir, "phase_1_copy_unmodified_files.sh")))) {
      copyUnmodifiedFiles(out, schemaVersions.getPhase(1));
    }
    try (final PrintStream out = new PrintStream(
        new FileOutputStream(new File(dir, "phase_2_copy_unmodified_files.sh")))) {
      copyUnmodifiedFiles(out, schemaVersions.getPhase(2));
    }
  }

  private void copyUnmodifiedFiles(final PrintStream out, final SchemaPhase phase) {
    final Map<String, DataSchemaTable> schema = phase.getSchema().getTables();
    final List<String> names = new ArrayList<String>(schema.keySet());
    Collections.sort(names);
    for (final String name : names) {
      final DataSchemaTable table = schema.get(name);
      if (!table.hasNewlyGeneratedElements()) {
        out.println("hadoop fs -cp /$1/" + table.getTableName() + " /$2/" + table.getTableName());
      }
    }
  }
}
