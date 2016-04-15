package edu.harvard.data.schema.existing;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExistingSchema {

  private final Map<String, ExistingSchemaTable> tables;

  @JsonCreator
  public ExistingSchema(@JsonProperty("tables") final Map<String, ExistingSchemaTable> tableMap) {
    this.tables = new HashMap<String, ExistingSchemaTable>();
    if (tableMap != null) {
      for (final String key : tableMap.keySet()) {
        tables.put(key, tableMap.get(key));
      }
    }
  }

  public Map<String, ExistingSchemaTable> getTables() {
    return tables;
  }
}
