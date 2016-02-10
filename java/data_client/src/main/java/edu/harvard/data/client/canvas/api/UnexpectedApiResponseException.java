package edu.harvard.data.client.canvas.api;

public class UnexpectedApiResponseException extends Exception {

  private static final long serialVersionUID = 1L;
  private final String url;
  private final int expected;
  private final int actual;

  public UnexpectedApiResponseException(final int expected, final int actual, final String url) {
    this.expected = expected;
    this.actual = actual;
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  @Override
  public String getMessage() {
    return url + " expected: " + expected + " received: " + actual;
  }

  public int getExpected() {
    return expected;
  }

  public int getActual() {
    return actual;
  }

}
