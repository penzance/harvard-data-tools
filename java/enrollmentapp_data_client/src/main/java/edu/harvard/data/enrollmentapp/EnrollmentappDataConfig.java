package edu.harvard.data.enrollmentapp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class EnrollmentappDataConfig extends DataConfig {

  private final String dropboxBucket;

  public EnrollmentappDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.dropboxBucket = getConfigParameter("dropbox_bucket", verify);
    this.codeGeneratorScript = "enrollmentapp_generate_tools.py";
    this.codeManagerClass = "edu.harvard.data.enrollmentapp.EnrollmentappCodeManager";
    if (verify) {
      checkParameters();
    }
  }

  public static EnrollmentappDataConfig parseFiles(final String[] configFiles)
      throws IOException, DataConfigurationException {
    final List<FileInputStream> streams = new ArrayList<FileInputStream>();
    for (final String file : configFiles) {
      streams.add(new FileInputStream(file));
    }
    final EnrollmentappDataConfig config = new EnrollmentappDataConfig(streams, true);
    for (final FileInputStream in : streams) {
      in.close();
    }
    return config;
  }

  public String getDropboxBucket() {
    return dropboxBucket;
  }
}
