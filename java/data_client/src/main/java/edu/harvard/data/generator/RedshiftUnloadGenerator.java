package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;

public class RedshiftUnloadGenerator {

  private final S3ObjectId workingDir;
  private final DataConfig config;
  private final File dir;

  public RedshiftUnloadGenerator(final File dir, final DataConfig config,
      final S3ObjectId workingDir) {
    this.config = config;
    this.dir = dir;
    this.workingDir = workingDir;
  }

  public void generate() throws IOException {
    final File unloadFile = new File(dir, config.getRedshiftUnloadScript());

    try (final PrintStream out = new PrintStream(new FileOutputStream(unloadFile))) {
      generateUnloadFile(out);
    }
  }

  private void generateUnloadFile(final PrintStream out) {
    final String query = "SELECT * FROM pii.identity_map";
    final S3ObjectId location = AwsUtils.key(workingDir, "unloaded_tables", "identity_map");
    final String credentials = "aws_access_key_id=" + config.getAwsKeyId()
    + ";aws_secret_access_key=" + config.getAwsSecretKey();
    out.println("UNLOAD ('" + query + "') TO '" + AwsUtils.uri(location)
    + "/' WITH CREDENTIALS AS '" + credentials + "' DELIMITER '\\t' NULL AS '\\\\N';");
  }
}
