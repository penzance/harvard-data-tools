package edu.harvard.data.canvas.phase_0;

public class ArgumentError extends Exception {

  private static final long serialVersionUID = 1L;

  ArgumentError(final String msg) {
    super(msg);
  }
}