package edu.harvard.data.edx;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class EdxDataConfig extends DataConfig {

  private final String dropboxBucket;

  public EdxDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.codeGeneratorScript = "edx_generate_tools.py";
    this.codeManagerClass = "edu.harvard.data.edx.EdxCodeManager";
    this.dropboxBucket = getConfigParameter("dropbox_bucket", verify);
    if (verify) {
      checkParameters();
    }
  }

  public String getDropboxBucket() {
    return dropboxBucket;
  }

}
