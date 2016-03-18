package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HiveQueryManifestGenerator {
  private static final Logger log = LogManager.getLogger();

  private final File gitDir;
  private final GenerationSpec schemaVersions;
  private final File dir;

  public HiveQueryManifestGenerator(final File gitDir, final File dir,
      final GenerationSpec schemaVersions) {
    this.gitDir = gitDir;
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    for (int i = 1; i <= 3; i++) {
      final String fileBase = "phase_" + i + "_hive";
      final File file = new File(dir, fileBase + ".sh");
      final File hiveDir = new File(gitDir, "hive/phase_" + i);
      log.info("Generating hive mainifest for phase " + i + ". file: " + file);
      log.info("Phase " + i + " hive directory: " + hiveDir);

      try (final PrintStream out = new PrintStream(new FileOutputStream(file))) {
        generateHiveManifest(out, schemaVersions.getPhase(i), hiveDir,
            "/home/hadoop/" + fileBase + ".out");
      }
    }
  }

  private void generateHiveManifest(final PrintStream out, final SchemaPhase phase,
      final File queryDir, final String logFile) {
    out.println("set -e"); // Exit on any failure
    out.println("sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug");
    out.println("sudo chown hive:hive -R /var/log/hive");
    if (queryDir.exists() && queryDir.isDirectory()) {
      for (final String fileName : queryDir.list()) {
        out.println("hive -f $1/" + fileName + " &>> " + logFile);
      }
    }
  }

}
