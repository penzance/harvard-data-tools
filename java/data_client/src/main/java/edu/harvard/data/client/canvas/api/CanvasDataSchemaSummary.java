package edu.harvard.data.client.canvas.api;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataSchemaSummary {
  private final String version;
  private final Date createdAt;

  @JsonCreator
  public CanvasDataSchemaSummary(@JsonProperty("version") final String version,
      @JsonProperty("createdAt") final Date createdAt) {
    this.version = version;
    this.createdAt = createdAt;
  }

  public String getVersion() {
    return version;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  @Override
  public String toString() {
    return "version:" + version + " createdAt:" + createdAt;
  }

}
