package edu.harvard.data.data_tool_generator.hive;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import edu.harvard.data.data_tool_generator.CanvasDataGenerator;
import edu.harvard.data.data_tool_generator.SchemaPhase;
import edu.harvard.data.data_tool_generator.SchemaTransformer;

public class HiveQueryManifestGenerator {
  private final File gitDir;
  private final SchemaTransformer schemaVersions;
  private final File dir;

  public HiveQueryManifestGenerator(final File gitDir, final File dir,
      final SchemaTransformer schemaVersions) {
    this.gitDir = gitDir;
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    final File phase1File = new File(dir, "canvas_phase_1_hive.q");
    final File phase2File = new File(dir, "canvas_phase_2_hive.q");

    try (final PrintStream out = new PrintStream(new FileOutputStream(phase1File))) {
      generateHiveManifest(out, schemaVersions.getPhase(1), new File(gitDir, "hive/phase_1"));
    }
    try (final PrintStream out = new PrintStream(new FileOutputStream(phase2File))) {
      generateHiveManifest(out, schemaVersions.getPhase(1), new File(gitDir, "hive/phase_2"));
    }
  }

  private void generateHiveManifest(final PrintStream out, final SchemaPhase phase,
      final File queryDir) {
    for (final String fileName : queryDir.list()) {
      out.println("source FILE ${" + CanvasDataGenerator.HDFS_HIVE_QUERY_DIR + "}/" + fileName + ";");
    }
  }

}
