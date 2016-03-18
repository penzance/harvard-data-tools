package edu.harvard.data.canvas.cli;

import edu.harvard.data.ReturnStatus;

public class ArgumentError extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private final ReturnStatus status;

  public ArgumentError(final ReturnStatus status, final String msg) {
    super(msg);
    this.status = status;
  }

  public ReturnStatus getStatus() {
    return status;
  }
}
