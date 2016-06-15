package edu.harvard.data;

public class NoInputDataException extends Exception {

  private static final long serialVersionUID = 1L;

  public NoInputDataException(final String jobName, final String inputPath) {
    super("Job " + jobName + " missing input data from " + inputPath);
  }


}
