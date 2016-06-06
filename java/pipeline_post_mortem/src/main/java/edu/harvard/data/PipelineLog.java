package edu.harvard.data;

public class PipelineLog {
  private final String label;
  private final String url;

  public PipelineLog(final String label, final String url) {
    this.label = label;
    this.url = url;
  }

  public String getLabel() {
    return label;
  }

  public String getUrl() {
    return url;
  }

}
