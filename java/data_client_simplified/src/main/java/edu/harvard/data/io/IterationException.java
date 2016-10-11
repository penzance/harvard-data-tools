package edu.harvard.data.io;

/**
 * Runtime wrapper class for checked exceptions thrown inside iterators that
 * implement the {@link Iterable} interface. Clients should use the
 * {@link #getCause} method to determine the original exception.
 */
public class IterationException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public IterationException(final Throwable cause) {
    super(cause);
  }
}
