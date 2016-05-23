package edu.harvard.data.io;

public class IterationException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public IterationException(final Exception cause) {
    super(cause);
  }
}
