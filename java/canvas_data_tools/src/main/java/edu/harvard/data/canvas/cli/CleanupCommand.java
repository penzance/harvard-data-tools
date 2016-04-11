package edu.harvard.data.canvas.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CleanupCommand implements Command {

  private static final Logger log = LogManager.getLogger();

  @Override
  public ReturnStatus execute(final DataConfiguration config, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException,
      VerificationException {
    log.info("Cleaning up");

    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Create and run the Canvas hadoop jobs for a single phase";
  }

}
