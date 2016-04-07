package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;

public class Phase1PostVerifier implements Verifier {
  private static final Logger log = LogManager.getLogger();

  public Phase1PostVerifier(final URI hdfsService, final String inputDir, final String outputDir,
      final String verifyDir) {
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running post-verifier for phase 1");
  }
}
