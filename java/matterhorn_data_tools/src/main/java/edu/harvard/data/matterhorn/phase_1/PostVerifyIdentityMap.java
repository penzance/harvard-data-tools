package edu.harvard.data.matterhorn.phase_1;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;

public class PostVerifyIdentityMap implements Verifier {
  private static final Logger log = LogManager.getLogger();
  private final String originalDir;
  private final String updatedDir;

  public PostVerifyIdentityMap(final Configuration hadoopConfig, final URI hdfsService,
      final String originalDir, final String updatedDir, final TableFormat format) {
    this.originalDir = originalDir;
    this.updatedDir = updatedDir;
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Verifying identity map. Original dir: " + originalDir + ", updated dir: " + updatedDir);
  }

}
