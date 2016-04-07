package edu.harvard.data.matterhorn.data_api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;

public class MatterhornDataSchema implements DataSchema {


  private Map<String, DataSchemaTable> tables;
  private Map<String, DataSchemaTable> tablesByName;

  @JsonCreator
  public MatterhornDataSchema(@JsonProperty("version") final String version,
      @JsonProperty("schema") final Map<String, CanvasDataSchemaTable> schema) {
  }

  public MatterhornDataSchema(final MatterhornDataSchema original) {
  }

  @Override
  public String getVersion() {
    return "1.0";
  }

  @Override
  public Map<String, DataSchemaTable> getTables() {
    return tables;
  }

  @Override
  public DataSchema copy() {
    return new MatterhornDataSchema(this);
  }

  @Override
  public DataSchemaTable getTableByName(final String name) {
    return tablesByName.get(name);
  }

  @Override
  public String toString() {
    String s = "";
    final List<String> tableNames = new ArrayList<String>(tablesByName.keySet());
    Collections.sort(tableNames);
    for (final String t : tableNames) {
      s += tablesByName.get(t) + "\n";
    }
    return s.trim();
  }
}
