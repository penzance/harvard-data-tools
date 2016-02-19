package edu.harvard.data.generator.schema;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.client.schema.TableOwner;

public class ExtensionSchemaTable extends DataSchemaTable {

  private final String description;
  private final String tableName;
  private final TableOwner owner;
  private final ArrayList<DataSchemaColumn> columns;

  @JsonCreator
  public ExtensionSchemaTable(@JsonProperty("description") final String description,
      @JsonProperty("columns") final List<ExtensionSchemaColumn> columnList,
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("owner") final TableOwner owner) {
    super(false, owner);
    this.description = description;
    this.tableName = tableName;
    this.owner = owner;
    this.columns = new ArrayList<DataSchemaColumn>();
    if (columnList != null) {
      for (final ExtensionSchemaColumn column : columnList) {
        this.columns.add(column);
      }
    }
  }

  public ExtensionSchemaTable(final ExtensionSchemaTable original) {
    super(original.newlyGenerated, original.owner);
    this.description = original.description;
    this.tableName = original.tableName;
    this.owner = original.owner;
    this.columns = new ArrayList<DataSchemaColumn>();
    for (final DataSchemaColumn column : original.columns) {
      this.columns.add(new ExtensionSchemaColumn((ExtensionSchemaColumn) column));
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

}
