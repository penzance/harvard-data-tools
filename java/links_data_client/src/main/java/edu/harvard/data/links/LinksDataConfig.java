package edu.harvard.data.links;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class LinksDataConfig extends DataConfig {

  private final String apiKey;
  private final String dropboxBucket;


  public LinksDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.apiKey = getConfigParameter("api_key", verify);
    this.dropboxBucket = getConfigParameter("dropbox_bucket", verify);


    this.codeGeneratorScript = "links_generate_tools.py";
    this.codeManagerClass = "edu.harvard.data.links.LinksCodeManager";

    if (verify) {
      checkParameters();
    }
  }

  public static LinksDataConfig parseFiles(final String[] configFiles)
      throws IOException, DataConfigurationException {
    final List<FileInputStream> streams = new ArrayList<FileInputStream>();
    for (final String file : configFiles) {
      streams.add(new FileInputStream(file));
    }
    final LinksDataConfig config = new LinksDataConfig(streams, true);
    for (final FileInputStream in : streams) {
      in.close();
    }
    return config;
  }

  public String getApiKey() {
    return apiKey;
  }
  
  public String getDropboxBucket() {
	    return dropboxBucket;
	  }
  
}
