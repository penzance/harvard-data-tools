package edu.harvard.data.client.canvas.api;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataFile {
  private final String filename;
  private final String url;
  private RestUtils rest;

  @JsonCreator
  public CanvasDataFile(@JsonProperty("url") final String url,
      @JsonProperty("filename") final String filename) {
    this.url = url;
    this.filename = filename;
  }

  void setRestUtils(final RestUtils rest) {
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
