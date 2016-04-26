package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.schema.DataSchemaTable;

public class MoveUnmodifiedTableGenerator {
  private static final Logger log = LogManager.getLogger();

  private final File dir;
  private final GenerationSpec schemaVersions;

  public MoveUnmodifiedTableGenerator(final File dir, final GenerationSpec schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    for (int i = 0; i < 3; i++) {
      final String fileBase = "phase_" + (i + 1) + "_move_unmodified_files";
      final File file = new File(dir, fileBase + ".sh");
      log.info("Generating move unmodified files for phase " + i + ". file: " + file);
      try (final PrintStream out = new PrintStream(new FileOutputStream(file))) {
        moveUnmodifiedFiles(out, i + 1, schemaVersions.getPhase(i), schemaVersions.getPhase(i+1),
            "/home/hadoop/" + fileBase + ".out");
      }
    }
  }

  private void moveUnmodifiedFiles(final PrintStream out, final int phase, final SchemaPhase inputPhase,
      final SchemaPhase outputPhase, final String logFile) {
    out.println("if ! hadoop fs -test -e " + outputPhase.getHDFSDir() + "; then hadoop fs -mkdir " + outputPhase.getHDFSDir() + "; fi");
    out.println("set -e"); // Exit on any failure
    out.println("sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug");
    out.println("sudo chown hive:hive -R /var/log/hive");
    final Map<String, DataSchemaTable> schema = outputPhase.getSchema().getTables();
    final List<String> names = new ArrayList<String>(schema.keySet());
    Collections.sort(names);
    for (final String name : names) {
      final DataSchemaTable table = schema.get(name);
      if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
        if (!table.hasNewlyGeneratedElements()) {
          out.println("hadoop fs -mv " + inputPhase.getHDFSDir() + "/" + table.getTableName() + " "
              + outputPhase.getHDFSDir() + "/" + table.getTableName() + " &>> " + logFile);
        }
      }
    }
  }
}
