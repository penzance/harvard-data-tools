package edu.harvard.data.client.generator.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.client.schema.TableOwner;

public class ExtensionSchemaTable extends DataSchemaTable {

  private final String description;
  private final String tableName;
  private final Map<String, DataSchemaColumn> columnsByName;
  private final List<DataSchemaColumn> columns;

  @JsonCreator
  public ExtensionSchemaTable(@JsonProperty("description") final String description,
      @JsonProperty("columns") final List<ExtensionSchemaColumn> columnList,
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("owner") final TableOwner owner) {
    super(false, owner);
    this.description = description;
    this.tableName = tableName;
    this.columns = new ArrayList<DataSchemaColumn>();
    this.columnsByName = new HashMap<String, DataSchemaColumn>();
    if (columnList != null) {
      for (final ExtensionSchemaColumn column : columnList) {
        this.columns.add(column);
        this.columnsByName.put(column.getName(), column);
      }
    }
  }

  public ExtensionSchemaTable(final String name, final List<DataSchemaColumn> columns) {
    super(false, null);
    this.tableName = name;
    this.description = null;
    this.columns = new ArrayList<DataSchemaColumn>();
    this.columnsByName = new HashMap<String, DataSchemaColumn>();
    for (final DataSchemaColumn column : columns) {
      final DataSchemaColumn columnCopy = column.copy();
      this.columns.add(column);
      this.columnsByName.put(columnCopy.getName(), columnCopy);
    }
  }

  private ExtensionSchemaTable(final ExtensionSchemaTable original) {
    super(original.newlyGenerated, original.owner);
    this.description = original.description;
    this.tableName = original.tableName;
    this.columns = new ArrayList<DataSchemaColumn>();
    this.columnsByName = new HashMap<String, DataSchemaColumn>();
    for (final DataSchemaColumn column : original.columns) {
      final DataSchemaColumn columnCopy = column.copy();
      this.columns.add(columnCopy);
      this.columnsByName.put(columnCopy.getName(), columnCopy);
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public List<DataSchemaColumn> getColumns() {
    return columns;
  }

  @Override
  public DataSchemaColumn getColumn(final String name) {
    return columnsByName.get(name);
  }

  @Override
  public DataSchemaTable copy() {
    return new ExtensionSchemaTable(this);
  }

  @Override
  public String toString() {
    String s = tableName + (newlyGenerated ? " *" : "");
    for (final DataSchemaColumn column : columns) {
      s += "\n    " + column;
    }
    return s;
  }
}
