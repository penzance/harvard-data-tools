package edu.harvard.data.canvas.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public interface Command {

  String getDescription();

  ReturnStatus execute(CanvasDataConfig config, ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException, ArgumentError;

}
