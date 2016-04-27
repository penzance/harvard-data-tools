package edu.harvard.data.identity;

/**
 * Exception thrown if some invariant under which a method should be called is
 * violated. This exception should not be seen in code that conforms to the
 * package interface.
 */
public class IdentityImplementationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public IdentityImplementationException(final String msg) {
    super(msg);
  }

}
