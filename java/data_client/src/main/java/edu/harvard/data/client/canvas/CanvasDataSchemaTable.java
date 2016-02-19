package edu.harvard.data.client.canvas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.client.schema.SchemaDifference;
import edu.harvard.data.client.schema.TableOwner;

public class CanvasDataSchemaTable extends DataSchemaTable {

  public enum DataWarehouseType {
    dimension, fact, both
  }

  private final DataWarehouseType dwType;
  private final String description;
  private final boolean incremental;
  private final String tableName;
  private final Map<String, String> hints; // "sort_key" : "timestamp" in
  // requests
  private List<DataSchemaColumn> columns;
  private final String seeAlso;
  private final String databasePath; // non-null in requests
  private final String originalTable; // non-null in requests


  @JsonCreator
  public CanvasDataSchemaTable(@JsonProperty("dw_type") final DataWarehouseType dwType,
      @JsonProperty("description") final String description,
      @JsonProperty("columns") final List<CanvasDataSchemaColumn> columnList,
      @JsonProperty("incremental") final boolean incremental,
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("hints") final Map<String, String> hints,
      @JsonProperty("data_base_path") final String databasePath,
      @JsonProperty("original_table") final String originalTable,
      @JsonProperty("see_also") final String seeAlso,
      @JsonProperty("owner") final TableOwner owner) {
    super(false, owner);
    this.dwType = dwType;
    this.description = description;
    this.incremental = incremental;
    this.tableName = tableName;
    this.hints = hints;
    this.databasePath = databasePath;
    this.originalTable = originalTable;
    this.seeAlso = seeAlso;
    this.owner = owner;
    this.columns = new ArrayList<DataSchemaColumn>();
    if (columnList != null) {
      for (final CanvasDataSchemaColumn column : columnList) {
        this.columns.add(column);
      }
    }
  }

  public CanvasDataSchemaTable(final CanvasDataSchemaTable original) {
    super(original.newlyGenerated, original.owner);
    this.dwType = original.dwType;
    this.description = original.description;
    this.incremental = original.incremental;
    this.tableName = original.tableName;
    this.hints = original.hints == null ? null : new HashMap<String, String>(original.hints);
    this.seeAlso = original.seeAlso;
    this.databasePath = original.databasePath;
    this.originalTable = original.originalTable;
    this.owner = original.owner;
    this.columns = new ArrayList<DataSchemaColumn>();
    for (final DataSchemaColumn column : original.columns) {
      this.columns.add(column.copy());
    }
  }

  @Override
  public DataSchemaTable copy() {
    return new CanvasDataSchemaTable(this);
  }

  public DataWarehouseType getDwType() {
    return dwType;
  }

  public String getDescription() {
    return description;
  }

  public boolean isIncremental() {
    return incremental;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public Map<String, String> getHints() {
    return hints;
  }

  @Override
  public List<DataSchemaColumn> getColumns() {
    return columns;
  }

  public String getSeeAlso() {
    return seeAlso;
  }

  public String getDatabasePath() {
    return databasePath;
  }

  public String getOriginalTable() {
    return originalTable;
  }

  public void calculateDifferences(final CanvasDataSchemaTable table2,
      final List<SchemaDifference> differences) {
    if (dwType != table2.dwType) {
      differences.add(new SchemaDifference("dwType", dwType, table2.dwType, tableName));
    }
    if (!compareStrings(description, table2.description)) {
      differences
      .add(new SchemaDifference("description", description, table2.description, tableName));
    }
    if (!compareStrings(tableName, table2.tableName)) {
      differences.add(new SchemaDifference("tableName", tableName, table2.tableName, tableName));
    }
    if (incremental != table2.incremental) {
      differences
      .add(new SchemaDifference("incremental", incremental, table2.incremental, tableName));
    }
    if (!compareStrings(seeAlso, table2.seeAlso)) {
      differences.add(new SchemaDifference("seeAlso", seeAlso, table2.seeAlso, tableName));
    }
    if (!compareStrings(databasePath, table2.databasePath)) {
      differences
      .add(new SchemaDifference("databasePath", databasePath, table2.databasePath, tableName));
    }
    if (!compareStrings(originalTable, table2.originalTable)) {
      differences.add(
          new SchemaDifference("originalTable", originalTable, table2.originalTable, tableName));
    }
    calculateHintDifferences(table2.hints, differences);
    calculateColumnDifferences(table2.columns, differences);
  }

  private boolean compareStrings(final String s1, final String s2) {
    if (s1 == null || s2 == null) {
      return s1 == s2;
    }
    return s1.equals(s2);
  }

  private void calculateColumnDifferences(final List<DataSchemaColumn> columns2,
      final List<SchemaDifference> differences) {
    final Map<String, CanvasDataSchemaColumn> map = new HashMap<String, CanvasDataSchemaColumn>();
    for (final DataSchemaColumn column : columns) {
      map.put(column.getName(), (CanvasDataSchemaColumn) column);
    }

    for (final DataSchemaColumn column : columns2) {
      final String columnName = column.getName();
      if (map.containsKey(columnName)) {
        map.get(columnName).calculateDifferences(tableName, (CanvasDataSchemaColumn) column, differences);
        map.remove(columnName);
      } else {
        differences.add(new SchemaDifference(tableName + ": Added column " + columnName));
      }
    }

    for (final String columnName : map.keySet()) {
      differences.add(new SchemaDifference(tableName + ": Removed column " + columnName));
    }
  }

  private void calculateHintDifferences(final Map<String, String> hints2,
      final List<SchemaDifference> differences) {
    for (final String hintKey : hints.keySet()) {
      final String hint2 = hints2.get(hintKey);
      if (hint2 == null) {
        differences.add(new SchemaDifference(tableName + ": Removed hint " + hintKey));
      } else {
        if (!hints.get(hintKey).equals(hint2)) {
          differences.add(new SchemaDifference(tableName + ": Changed hint " + hintKey + " from "
              + hints.get(hintKey) + " to " + hint2));
        }
      }
    }
    for (final String hintKey : hints2.keySet()) {
      if (!hints.containsKey(hintKey)) {
        differences.add(new SchemaDifference(tableName + ": Added hint " + hintKey));
      }
    }
  }

  public void setColumns(final ArrayList<DataSchemaColumn> newColumns) {
    this.columns = newColumns;
  }
}
