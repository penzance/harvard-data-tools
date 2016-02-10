package edu.harvard.canvas_data.aws_data_tools.cli;

import java.io.IOException;

import edu.harvard.canvas_data.aws_data_tools.VerificationException;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.api.UnexpectedApiResponseException;

public interface Command {

  String getDescription();

  ReturnStatus execute(DataConfiguration config) throws IOException, UnexpectedApiResponseException,
  DataConfigurationException, VerificationException;

}
