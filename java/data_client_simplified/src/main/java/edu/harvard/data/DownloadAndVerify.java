package edu.harvard.data;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import edu.harvard.data.schema.UnexpectedApiResponseException;

public abstract class DownloadAndVerify {

  protected abstract InputTableIndex run()
      throws IOException, InterruptedException, ExecutionException, DataConfigurationException,
      UnexpectedApiResponseException, VerificationException, ArgumentError;

}
