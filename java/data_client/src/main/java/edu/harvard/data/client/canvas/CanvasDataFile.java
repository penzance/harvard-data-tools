package edu.harvard.data.client.canvas;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.UnexpectedApiResponseException;

public class CanvasDataFile {
  private final String filename;
  private final String url;
  private CanvasRestUtils rest;

  @JsonCreator
  public CanvasDataFile(@JsonProperty("url") final String url,
      @JsonProperty("filename") final String filename) {
    this.url = url;
    this.filename = filename;
  }

  void setRestUtils(final CanvasRestUtils rest) {
    this.rest = rest;
  }

  @JsonIgnore
  public String getUrl() {
    return url;
  }

  public String getFilename() {
    return filename;
  }

  public void download(final File dest) throws IOException, UnexpectedApiResponseException {
    dest.getParentFile().mkdirs();
    rest.downloadFile(url, dest, 200);
  }

}
