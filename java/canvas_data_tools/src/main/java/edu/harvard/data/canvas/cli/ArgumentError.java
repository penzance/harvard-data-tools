package edu.harvard.data.canvas.cli;

public class ArgumentError extends Exception {

  private static final long serialVersionUID = 1L;

  public ArgumentError(final String msg) {
    super(msg);
  }
}
