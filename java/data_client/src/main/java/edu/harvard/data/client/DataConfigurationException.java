package edu.harvard.data.client;

public class DataConfigurationException extends Exception {

  private static final long serialVersionUID = 1L;

  public DataConfigurationException(final Throwable src) {
    super(src);
  }

  public DataConfigurationException(final String msg) {
    super(msg);
  }

}
