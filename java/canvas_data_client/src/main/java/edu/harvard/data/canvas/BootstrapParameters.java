package edu.harvard.data.canvas;

public class BootstrapParameters {
  private String configPathString;
  private Integer dumpSequence;
  private String table;
  private boolean downloadOnly;

  public String getConfigPathString() {
    return configPathString;
  }

  public void setConfigPathString(final String configPathString) {
    this.configPathString = configPathString;
  }

  public Integer getDumpSequence() {
    return dumpSequence;
  }

  public void setDumpSequence(final Integer dumpSequence) {
    this.dumpSequence = dumpSequence;
  }

  public String getTable() {
    return table;
  }

  public void setTable(final String table) {
    this.table = table;
  }

  public boolean getDownloadOnly() {
    return downloadOnly;
  }

  public void setDownloadOnly(final boolean downloadOnly) {
    this.downloadOnly = downloadOnly;
  }

}
