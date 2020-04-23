package edu.harvard.data.remoteapp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class RemoteappDataConfig extends DataConfig {

  private final String dropboxBucket;

  public RemoteappDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.dropboxBucket = getConfigParameter("dropbox_bucket", verify);
    this.codeGeneratorScript = "remoteapp_generate_tools.py";
    this.codeManagerClass = "edu.harvard.data.remoteapp.RemoteappCodeManager";
    if (verify) {
      checkParameters();
    }
  }

  public static RemoteappDataConfig parseFiles(final String[] configFiles)
      throws IOException, DataConfigurationException {
    final List<FileInputStream> streams = new ArrayList<FileInputStream>();
    for (final String file : configFiles) {
      streams.add(new FileInputStream(file));
    }
    final RemoteappDataConfig config = new RemoteappDataConfig(streams, true);
    for (final FileInputStream in : streams) {
      in.close();
    }
    return config;
  }

  public String getDropboxBucket() {
    return dropboxBucket;
  }
}
