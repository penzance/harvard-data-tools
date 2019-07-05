package edu.harvard.data.linksapp;

public class BootstrapParameters {
  private String configPathString;
  private boolean createPipeline=false;
  private String message;

  public String getConfigPathString() {
    return configPathString;
  }

  public void setConfigPathString(final String configPathString) {
    this.configPathString = configPathString;
  }

  public boolean getCreatePipeline() {
    return createPipeline;
  }

  public void setCreatePipeline(final boolean createPipeline) {
    this.createPipeline = createPipeline;
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
        + message + "\n  createPipeline: " + createPipeline;
  }

}
