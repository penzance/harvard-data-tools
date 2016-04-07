package edu.harvard.data.schema.extension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;

public class ExtensionSchema implements DataSchema {

  private final Map<String, DataSchemaTable> tables;

  @JsonCreator
  public ExtensionSchema(@JsonProperty("tables") final Map<String, ExtensionSchemaTable> tableMap) {
    this.tables = new HashMap<String, DataSchemaTable>();
    if (tableMap != null) {
      for (final String key : tableMap.keySet()) {
        tables.put(key, tableMap.get(key));
      }
    }
  }

  private ExtensionSchema(final ExtensionSchema original) {
    this.tables = new HashMap<String, DataSchemaTable>();
    for (final String key : original.tables.keySet()) {
      tables.put(key, original.tables.get(key).copy());
    }
  }

  @Override
  public Map<String, DataSchemaTable> getTables() {
    return tables;
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public DataSchema copy() {
    return new ExtensionSchema(this);
  }

  @Override
  public DataSchemaTable getTableByName(final String name) {
    return tables.get(name);
  }

  @Override
  public String toString() {
    String s = "";
    final List<String> tableNames = new ArrayList<String>(tables.keySet());
    Collections.sort(tableNames);
    for (final String t : tableNames) {
      s += tables.get(t) + "\n";
    }
    return s.trim();
  }

}
