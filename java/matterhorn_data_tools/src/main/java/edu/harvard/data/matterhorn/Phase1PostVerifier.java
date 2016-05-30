package edu.harvard.data.matterhorn;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;
import edu.harvard.data.matterhorn.MatterhornDataConfig;

public class Phase1PostVerifier implements Verifier {
  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, VerificationException {
    final String configPathString = args[0];
    final MatterhornDataConfig config = MatterhornDataConfig.parseInputFiles(MatterhornDataConfig.class,
        configPathString, true);
    new Phase1PostVerifier(config).verify();
  }

  public Phase1PostVerifier(final MatterhornDataConfig config) throws DataConfigurationException {
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running post-verifier for phase 1");
  }
}
