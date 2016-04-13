package edu.harvard.data.canvas.data_api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.SchemaDifference;

public class CanvasDataSchema implements DataSchema {
  private final String version;
  private final Map<String, DataSchemaTable> tables;
  private final Map<String, DataSchemaTable> tablesByName;


  @JsonCreator
  public CanvasDataSchema(@JsonProperty("version") final String version,
      @JsonProperty("schema") final Map<String, CanvasDataSchemaTable> schema) {
    this.version = version;
    this.tables = new HashMap<String, DataSchemaTable>();
    this.tablesByName = new HashMap<String, DataSchemaTable>();
    if (schema != null) {
      for(final String key : schema.keySet()) {
        final DataSchemaTable table = schema.get(key);
        this.tables.put(key, table);
        this.tablesByName.put(table.getTableName(), table);
      }
    }
  }

  public CanvasDataSchema(final CanvasDataSchema original) {
    this.version = original.version;
    this.tables = new HashMap<String, DataSchemaTable>();
    this.tablesByName = new HashMap<String, DataSchemaTable>();
    for (final String tableName : original.tables.keySet()) {
      final DataSchemaTable table = original.tables.get(tableName).copy();
      this.tables.put(tableName, table);
      this.tablesByName.put(table.getTableName(), table);
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

  public List<SchemaDifference> diff(final DataSchema schema2) {
    final List<SchemaDifference> differences = new ArrayList<SchemaDifference>();
    final List<String> tableKeys = new ArrayList<String>(tables.keySet());
    Collections.sort(tableKeys);
    for (final String tableKey : tableKeys) {
      final CanvasDataSchemaTable table = (CanvasDataSchemaTable) tables.get(tableKey);
      if (schema2.getTables().containsKey(tableKey)) {
        table.calculateDifferences((CanvasDataSchemaTable) schema2.getTables().get(tableKey), differences);
      } else {
        differences.add(new SchemaDifference("Dropped table " + table.getTableName()));
      }
    }

    final List<String> tableKeys2 = new ArrayList<String>(schema2.getTables().keySet());
    Collections.sort(tableKeys2);
    for (final String key : tableKeys2) {
      if (!tableKeys.contains(key)) {
        differences
        .add(new SchemaDifference("Added table " +  schema2.getTables().get(key).getTableName()));
      }
    }
    return differences;
  }

  @Override
  public DataSchema copy() {
    return new CanvasDataSchema(this);
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

  @Override
  public void addTable(final String tableName, final DataSchemaTable newTable) {
    tables.put(tableName, newTable);
    tablesByName.put(tableName, newTable);
  }
}
