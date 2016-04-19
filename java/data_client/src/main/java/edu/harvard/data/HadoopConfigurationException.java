package edu.harvard.data;

public class HadoopConfigurationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public HadoopConfigurationException(final String msg) {
    super(msg);
  }

  public HadoopConfigurationException(final String msg, final Throwable e) {
    super(msg, e);
  }

}
