package edu.harvard.data.client.schema;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class DataSchemaTable {

  @JsonIgnore
  protected boolean newlyGenerated;

  public abstract String getTableName();

  public abstract List<DataSchemaColumn> getColumns();

  public abstract TableOwner getOwner();

  public abstract void setOwner(TableOwner owner);

  protected DataSchemaTable(final boolean newlyGenerated) {
    this.newlyGenerated = newlyGenerated;
  }

  public boolean getNewlyGenerated() {
    return newlyGenerated;
  }

  public boolean hasNewlyGeneratedElements() {
    for(final DataSchemaColumn column : getColumns()) {
      if (column.getNewlyGenerated()) {
        return true;
      }
    }
    return newlyGenerated;
  }

  public void setNewlyGenerated(final boolean newlyGenerated) {
    this.newlyGenerated = newlyGenerated;
  }

}
