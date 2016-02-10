package edu.harvard.canvas_data.aws_data_tools;

import edu.harvard.canvas_data.aws_data_tools.cli.ReturnStatus;

public class FatalError extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private final ReturnStatus status;

  public FatalError(final ReturnStatus status, final String msg) {
    super(msg);
    this.status = status;
  }

  public ReturnStatus getStatus() {
    return status;
  }
}
