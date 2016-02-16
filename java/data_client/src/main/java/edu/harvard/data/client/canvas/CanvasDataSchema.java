package edu.harvard.data.client.canvas;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchema;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.client.schema.SchemaDifference;

public class CanvasDataSchema implements DataSchema {
  private final String version;
  private final Map<String, DataSchemaTable> tables;

  @JsonCreator
  public CanvasDataSchema(@JsonProperty("version") final String version,
      @JsonProperty("schema") final Map<String, CanvasDataSchemaTable> schema) {
    this.version = version;
    this.tables = new HashMap<String, DataSchemaTable>();
    if (schema != null) {
      for(final String key : schema.keySet()) {
        tables.put(key, schema.get(key));
      }
    }
  }

  public CanvasDataSchema(final CanvasDataSchema original) {
    this.version = original.version;
    this.tables = new HashMap<String, DataSchemaTable>();
    for (final String tableName : original.tables.keySet()) {
      this.tables.put(tableName,
          new CanvasDataSchemaTable((CanvasDataSchemaTable) original.tables.get(tableName)));
    }
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public Map<String, DataSchemaTable> getTables() {
    return tables;
  }

  public List<SchemaDifference> calculateDifferences(final CanvasDataSchema schema2) {
    final List<SchemaDifference> differences = new ArrayList<SchemaDifference>();
    final List<String> tableKeys = new ArrayList<String>(tables.keySet());
    Collections.sort(tableKeys);
    for (final String tableKey : tableKeys) {
      final CanvasDataSchemaTable table = (CanvasDataSchemaTable) tables.get(tableKey);
      if (schema2.tables.containsKey(tableKey)) {
        table.calculateDifferences((CanvasDataSchemaTable) schema2.tables.get(tableKey), differences);
      } else {
        differences.add(new SchemaDifference("Dropped table " + table.getTableName()));
      }
    }

    final List<String> tableKeys2 = new ArrayList<String>(schema2.tables.keySet());
    Collections.sort(tableKeys2);
    for (final String key : tableKeys2) {
      if (!tableKeys.contains(key)) {
        differences
        .add(new SchemaDifference("Added table " +  schema2.tables.get(key).getTableName()));
      }
    }
    return differences;
  }

  @Override
  public DataSchema copy() {
    return new CanvasDataSchema(this);
  }

}
