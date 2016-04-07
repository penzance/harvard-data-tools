package edu.harvard.data.canvas.phase_0;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;

public class Phase0PostVerifier implements Verifier {

  private static final Logger log = LogManager.getLogger();

  public Phase0PostVerifier(final String dumpId, final AwsUtils aws, final File tmpDir,
      final ExecutorService exec) {
  }

  @Override
  public void verify() throws VerificationException, IOException {
    log.info("Running pre-verifier for phase 0");
  }

}