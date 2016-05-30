package edu.harvard.data.matterhorn.phase_1;

import java.io.IOException;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;
import edu.harvard.data.matterhorn.MatterhornDataConfig;

public class Phase1PreVerifier implements Verifier {
  private static final Logger log = LogManager.getLogger();

  public Phase1PreVerifier(final MatterhornDataConfig dataConfig, final URI hdfsService,
      final String inputDir, final String outputDir) {
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running pre-verifier for phase 1");
  }

}
