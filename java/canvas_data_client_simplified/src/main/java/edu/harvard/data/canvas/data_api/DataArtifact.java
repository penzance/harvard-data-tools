package edu.harvard.data.canvas.data_api;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DataArtifact {
  private final String tableName;
  private final boolean partial;
  private final List<DataFile> files;

  @JsonCreator
  public DataArtifact(@JsonProperty("tableName") final String tableName,
      @JsonProperty("partial") final boolean partial,
      @JsonProperty("files") final List<DataFile> files) {
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

  public List<DataFile> getFiles() {
    return Collections.unmodifiableList(files);
  }

  public void downloadAllFiles(final File directory) throws IOException, UnexpectedApiResponseException {
    // Iterate over the files for the table.
    for (final DataFile dataFile : files) {
      final File dest = new File(directory, dataFile.getFilename());
      // Download the file.
      dataFile.download(dest);
    }
  }

  void setRestUtils(final RestUtils rest) {
    if (files != null) {
      for (final DataFile file : files) {
        file.setRestUtils(rest);
      }
    }
  }

  @Override
  public String toString() {
    return "name:" + tableName + " partial:" + partial + " files:" + files.size();
  }
}
