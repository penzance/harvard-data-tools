package edu.harvard.data.matterhorn;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class MatterhornDataConfig extends DataConfig {

  public final String dropboxBucket;

  public MatterhornDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.dropboxBucket = getConfigParameter("dropbox_bucket", verify);
    this.codeGeneratorScript = "matterhorn_generate_tools.py";
    this.pipelineSetupClass = "edu.harvard.data.matterhorn.MatterhornPipelineSetup";
    this.phase0Class = "edu.harvard.data.matterhorn.MatterhornPhase0";
    if (verify) {
      checkParameters();
    }
  }

  public static MatterhornDataConfig parseFiles(final String[] configFiles)
      throws IOException, DataConfigurationException {
    final List<FileInputStream> streams = new ArrayList<FileInputStream>();
    for (final String file : configFiles) {
      streams.add(new FileInputStream(file));
    }
    final MatterhornDataConfig config = new MatterhornDataConfig(streams, true);
    for (final FileInputStream in : streams) {
      in.close();
    }
    return config;
  }
}
