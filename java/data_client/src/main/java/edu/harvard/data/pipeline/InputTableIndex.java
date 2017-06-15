package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.generator.SchemaPhase;
import edu.harvard.data.schema.DataSchemaTable;

public class InputTableIndex {
  private String schemaVersion;
  private final Map<String, List<String>> tables;
  private final Map<String, Long> fileSizes;
  private final Map<String, Boolean> partial;

  public InputTableIndex() {
    this.tables = new HashMap<String, List<String>>();
    this.partial = new HashMap<String, Boolean>();
    this.fileSizes = new HashMap<String, Long>();
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(final String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public void addFile(final String table, final S3ObjectId file, final long sizeBytes) {
    if (!tables.containsKey(table)) {
      tables.put(table, new ArrayList<String>());
    }
    tables.get(table).add(AwsUtils.uri(file));
    fileSizes.put(AwsUtils.uri(file), sizeBytes);
  }

  public Map<String, List<String>> getTables() {
    return tables;
  }

  public List<String> getTableNames() {
    final List<String> names = new ArrayList<String>(tables.keySet());
    Collections.sort(names);
    return names;
  }

  public List<S3ObjectId> getFiles(final String table) {
    final List<S3ObjectId> files = new ArrayList<S3ObjectId>();
    for (final String file : tables.get(table)) {
      files.add(AwsUtils.key(file));
    }
    return files;
  }

  public void addAll(final InputTableIndex data) {
    fileSizes.putAll(data.fileSizes);
    for (final String table : data.tables.keySet()) {
      if (!tables.containsKey(table)) {
        tables.put(table, new ArrayList<String>());
      }
      tables.get(table).addAll(data.tables.get(table));
    }
  }

  public void addTable(final String table, final InputTableIndex data) {
    if (!tables.containsKey(table)) {
      tables.put(table, new ArrayList<String>());
    }
    tables.get(table).addAll(data.tables.get(table));
    for (final String file : data.tables.get(table)) {
      fileSizes.put(file, data.fileSizes.get(file));
    }
  }

  public Map<String, Long> getFileSizes() {
    return fileSizes;
  }

  public Long getFileSize(final S3ObjectId file) {
    return fileSizes.get(AwsUtils.uri(file));
  }

  @Override
  public String toString() {
    String s = "Schema: " + schemaVersion + "\n";
    for (final String key : getTableNames()) {
      s += key + ":\n";
      for (final S3ObjectId file : getFiles(key)) {
        s += "  " + AwsUtils.uri(file) + "\n";
      }
    }
    return s;
  }

  public boolean containsTable(final String table) {
    return tables.containsKey(table);
  }

  public static InputTableIndex read(final AwsUtils aws, final S3ObjectId location)
      throws IOException {
    return aws.readJson(location, InputTableIndex.class);
  }

  public void setPartial(final String table, final boolean isPartial) {
    partial.put(table, isPartial);
  }

  public void setPartial(final Map<String, Boolean> data) {
    this.partial.putAll(data);
  }

  public Map<String, Boolean> getPartial() {
    return partial;
  }

  public boolean isPartial(final String tableName) {
    return partial.get(tableName);
  }

  public boolean canvasMegadump() {
    return containsTable("requests") && !isPartial("requests");
  }

  public void addNewlyGeneratedTables(final List<SchemaPhase> schemaPhases) {
    for (final SchemaPhase phase : schemaPhases) {
      for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
        if (table.getNewlyGenerated()) {
          this.tables.put(table.getTableName(), new ArrayList<String>());
          this.partial.put(table.getTableName(), false); // XXX Need to figure this out per-table
          this.fileSizes.put(table.getTableName(), 0L);
        }
      }
    }
  }
}
