package edu.harvard.data.client.canvas.api;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataHistoricalDump {
  private final String dumpId;
  private final long sequence;
  private final boolean partial;
  private final List<CanvasDataFile> files;

  @JsonCreator
  public CanvasDataHistoricalDump(@JsonProperty("dumpId") final String dumpId,
      @JsonProperty("sequence") final long sequence,
      @JsonProperty("partial") final boolean partial,
      @JsonProperty("files") final List<CanvasDataFile> files) {
    this.dumpId = dumpId;
    this.sequence = sequence;
    this.partial = partial;
    this.files = files;
  }

  public String getDumpId() {
    return dumpId;
  }

  public long getSequence() {
    return sequence;
  }

  public boolean isPartial() {
    return partial;
  }

  public List<CanvasDataFile> getFiles() {
    return Collections.unmodifiableList(files);
  }

  public void setRestUtils(final RestUtils rest) {
    for (final CanvasDataFile file : files) {
      file.setRestUtils(rest);
    }
  }

  @Override
  public String toString() {
    return "id:" + dumpId + " sequence:" + sequence + " partial:" + partial + " files:" + files.size();
  }

}
