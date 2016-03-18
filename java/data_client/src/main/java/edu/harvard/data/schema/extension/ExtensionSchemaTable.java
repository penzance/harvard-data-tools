package edu.harvard.data.schema.extension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.TableOwner;

public class ExtensionSchemaTable extends DataSchemaTable {

  private final String like;
  private final String description;
  private String tableName;
  private final Map<String, DataSchemaColumn> columnsByName;
  private final List<DataSchemaColumn> columns;

  @JsonCreator
  public ExtensionSchemaTable(@JsonProperty("like") final String like,
      @JsonProperty("description") final String description,
      @JsonProperty("columns") final List<ExtensionSchemaColumn> columnList,
      @JsonProperty("owner") final TableOwner owner) {
    super(false, owner);
    this.like = like;
    this.description = description;
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
    this.like = null;
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
    this.like = original.like;
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

  public void setTableName(final String tableName) {
    this.tableName = tableName;
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
  public String getLikeTable() {
    return like;
  }

  @Override
  public DataSchemaTable copy() {
    return new ExtensionSchemaTable(this);
  }

  @Override
  public String toString() {
    String s = tableName + (newlyGenerated ? " *" : "");
    if (like != null) {
      s += " like " + like;
    }
    for (final DataSchemaColumn column : columns) {
      s += "\n    " + column;
    }
    return s;
  }
}