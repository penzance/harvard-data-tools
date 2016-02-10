package edu.harvard.data.client.canvas.api;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataArtifact {
  private final String tableName;
  private final boolean partial;
  private final List<CanvasDataFile> files;

  @JsonCreator
  public CanvasDataArtifact(@JsonProperty("tableName") final String tableName,
      @JsonProperty("partial") final boolean partial,
      @JsonProperty("files") final List<CanvasDataFile> files) {
    this.tableName = tableName;
    this.partial = partial;
    this.files = files;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isPartial() {
    return partial;
  }

  public List<CanvasDataFile> getFiles() {
    return Collections.unmodifiableList(files);
  }

  public void downloadAllFiles(final File directory) throws IOException, UnexpectedApiResponseException {
    // Iterate over the files for the table.
    for (final CanvasDataFile dataFile : files) {
      final File dest = new File(directory, dataFile.getFilename());
      // Download the file.
      dataFile.download(dest);
    }
  }

  void setRestUtils(final RestUtils rest) {
    if (files != null) {
      for (final CanvasDataFile file : files) {
        file.setRestUtils(rest);
      }
    }
  }

  @Override
  public String toString() {
    return "name:" + tableName + " partial:" + partial + " files:" + files.size();
  }
}
