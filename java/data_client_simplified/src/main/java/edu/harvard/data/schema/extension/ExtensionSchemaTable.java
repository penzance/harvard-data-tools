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
      @JsonProperty("owner") final TableOwner owner,
      @JsonProperty("expire_after_phase") final Integer expireAfterPhase) {
    super(false, owner, expireAfterPhase);
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
    super(false, null, null);
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
    super(original.newlyGenerated, original.owner, original.expireAfterPhase);
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

  public void setExpiration(final int phase) {
    this.expireAfterPhase = phase;
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
  public void removeColumn(final String name) {
    final DataSchemaColumn column = columnsByName.get(name);
    columns.remove(column);
    columnsByName.remove(column);
  }

  @Override
  public void addColumn(final DataSchemaColumn column) {
    columns.add(column);
    columnsByName.put(column.getName(), column);
  }

  @Override
  public void updateColumn(final DataSchemaColumn column) {
    final DataSchemaColumn oldColumn = columnsByName.get(column.getName());
    final int idx = columns.indexOf(oldColumn);
    columns.remove(oldColumn);
    columns.add(idx, column);
    columnsByName.put(column.getName(), column);
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
