package edu.harvard.data.generator.schema;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchema;
import edu.harvard.data.client.schema.DataSchemaTable;

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

  @JsonCreator
  private ExtensionSchema(final ExtensionSchema original) {
    this.tables = new HashMap<String, DataSchemaTable>();
    for (final String key : original.tables.keySet()) {
      tables.put(key, original.tables.get(key));
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

}
