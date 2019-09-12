package edu.harvard.data.midas;

public class BootstrapParameters {
  private String configPathString;
  private boolean downloadOnly;
  private String message;

  public String getConfigPathString() {
    return configPathString;
  }

  public void setConfigPathString(final String configPathString) {
    this.configPathString = configPathString;
  }

  public boolean getDownloadOnly() {
    return downloadOnly;
  }

  public void setDownloadOnly(final boolean downloadOnly) {
    this.downloadOnly = downloadOnly;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(final String message) {
    this.message = message;
  }

  @Override
  public String toString() {
    return "BootstrapParams\n  ConfigPath: " + configPathString + "\n  message: "
        + message + "\n  downloadOnly: " + downloadOnly;
  }

}
