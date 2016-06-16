package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;

public class InputTableIndex {
  private String schemaVersion;
  private final Map<String, List<String>> tables;
  private final Map<String, Boolean> partial;

  public InputTableIndex() {
    this.tables = new HashMap<String, List<String>>();
    this.partial = new HashMap<String, Boolean>();
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(final String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public void addDirectory(final String table, final S3ObjectId directory) {
    if (!tables.containsKey(table)) {
      tables.put(table, new ArrayList<String>());
    }
    tables.get(table).add(AwsUtils.uri(directory));
  }

  public void addDirectories(final String table, final List<S3ObjectId> directories) {
    for (final S3ObjectId dir : directories) {
      addDirectory(table, dir);
    }
  }

  public Map<String, List<String>> getTables() {
    return tables;
  }

  public List<String> getTableNames() {
    final List<String> names = new ArrayList<String>(tables.keySet());
    Collections.sort(names);
    return names;
  }

  public List<String> getDirectories(final String table) {
    return tables.get(table);
  }

  public void addTables(final Map<String, List<S3ObjectId>> tableMap) {
    for (final String table : tableMap.keySet()) {
      addDirectories(table, tableMap.get(table));
    }
  }

  @Override
  public String toString() {
    String s = "Schema: " + schemaVersion + "\n";
    for (final String key : getTableNames()) {
      s += key + ":\n";
      for (final String dir : getDirectories(key)) {
        s += "  " + dir + "\n";
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
    System.out.println(partial);
    return partial.get(tableName);
  }
}
