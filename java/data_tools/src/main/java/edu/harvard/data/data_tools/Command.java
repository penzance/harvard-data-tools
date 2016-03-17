package edu.harvard.data.data_tools;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.VerificationException;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;

public interface Command {

  String getDescription();

  ReturnStatus execute(DataConfiguration config, ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException;

}
