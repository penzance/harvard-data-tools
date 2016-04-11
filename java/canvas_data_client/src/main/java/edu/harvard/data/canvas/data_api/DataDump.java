package edu.harvard.data.canvas.data_api;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DataDump {
  private final String dumpId;
  private final long sequence;
  private final String accountId;
  private final int numFiles;
  private final boolean finished;
  private final Date expires;
  private final Date createdAt;
  private final Date updatedAt;
  private final String schemaVersion;
  private final Map<String, DataArtifact> artifactsByTable;

  @JsonCreator
  public DataDump(@JsonProperty("dumpId") final String dumpId,
      @JsonProperty("sequence") final long sequence,
      @JsonProperty("accountId") final String accountId,
      @JsonProperty("numFiles") final int numFiles,
      @JsonProperty("finished") final boolean finished, @JsonProperty("expires") final Date expires,
      @JsonProperty("createdAt") final Date createdAt,
      @JsonProperty("updatedAt") final Date updatedAt,
      @JsonProperty("schemaVersion") final String schemaVersion,
      @JsonProperty("artifactsByTable") final Map<String, DataArtifact> artifactsByTable) {
    this.dumpId = dumpId;
    this.sequence = sequence;
    this.accountId = accountId;
    this.numFiles = numFiles;
    this.finished = finished;
    this.expires = expires;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.schemaVersion = schemaVersion;
    this.artifactsByTable = artifactsByTable;
  }

  public String getDumpId() {
    return dumpId;
  }

  public long getSequence() {
    return sequence;
  }

  public String getAccountId() {
    return accountId;
  }

  public int getNumFiles() {
    return numFiles;
  }

  public boolean isFinished() {
    return finished;
  }

  public Date getExpires() {
    return expires;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public Map<String, DataArtifact> getArtifactsByTable() {
    if (artifactsByTable == null) {
      return null;
    }
    return Collections.unmodifiableMap(artifactsByTable);
  }

  public void downloadAllFiles(final File directory)
      throws IOException, UnexpectedApiResponseException {
    for (final String table : artifactsByTable.keySet()) {
      final DataArtifact artifact = artifactsByTable.get(table);
      artifact.downloadAllFiles(new File(directory, table));
    }
  }

  void setRestUtils(final RestUtils rest) {
    if (artifactsByTable != null) {
      for (final DataArtifact artifact : artifactsByTable.values()) {
        artifact.setRestUtils(rest);
      }
    }
  }

  @Override
  public String toString() {
    String str = "sequence:" + sequence + " id:" + dumpId + " schema: " + schemaVersion
        + " account:" + accountId + " tables:" + numFiles + " finished:" + finished + "\n  created:"
        + createdAt + "\n  updated:" + updatedAt + "\n  expires:" + expires + "\n";
    if (artifactsByTable != null) {
      for (final String key : artifactsByTable.keySet()) {
        final DataArtifact table = artifactsByTable.get(key);
        str += "    " + table + "\n";
      }
    }
    return str;
  }

  public int countFilesToDownload() {
    int files = 0;
    for (final DataArtifact artifact : artifactsByTable.values()) {
      files += artifact.getFiles().size();
    }
    return files;
  }
}
