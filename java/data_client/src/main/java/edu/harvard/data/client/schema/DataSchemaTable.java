package edu.harvard.data.client.schema;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class DataSchemaTable {

  @JsonIgnore
  protected boolean newlyGenerated;
  protected TableOwner owner;

  public abstract String getTableName();

  public abstract List<DataSchemaColumn> getColumns();

  public abstract DataSchemaTable copy();

  public abstract DataSchemaColumn getColumn(final String name);

  protected DataSchemaTable(final boolean newlyGenerated, final TableOwner owner) {
    this.newlyGenerated = newlyGenerated;
    this.owner = owner;
  }

  public boolean getNewlyGenerated() {
    return newlyGenerated;
  }

  public boolean hasNewlyGeneratedElements() {
    for (final DataSchemaColumn column : getColumns()) {
      if (column.getNewlyGenerated()) {
        return true;
      }
    }
    return newlyGenerated;
  }

  public void setNewlyGenerated(final boolean newlyGenerated) {
    this.newlyGenerated = newlyGenerated;
  }

  public TableOwner getOwner() {
    return owner;
  }

  public void setOwner(final TableOwner owner) {
    this.owner = owner;
  }

}
