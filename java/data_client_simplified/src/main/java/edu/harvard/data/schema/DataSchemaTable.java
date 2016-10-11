package edu.harvard.data.schema;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class DataSchemaTable {

  @JsonIgnore
  protected boolean newlyGenerated;
  protected TableOwner owner;
  protected Integer expireAfterPhase;

  public abstract String getTableName();

  public abstract List<DataSchemaColumn> getColumns();

  public abstract DataSchemaColumn getColumn(final String name);

  public abstract void addColumn(DataSchemaColumn column);

  public abstract DataSchemaTable copy();


  public abstract String getLikeTable();

  public abstract void removeColumn(final String name);

  public abstract void updateColumn(DataSchemaColumn column);

  protected DataSchemaTable(final boolean newlyGenerated, final TableOwner owner, final Integer expireAfterPhase) {
    this.newlyGenerated = newlyGenerated;
    this.owner = owner;
    this.expireAfterPhase = expireAfterPhase;
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

  public boolean isTemporary() {
    return expireAfterPhase != null;
  }

  public Integer getExpirationPhase() {
    return expireAfterPhase;
  }

  public void setExpirationPhase(final int phase) {
    this.expireAfterPhase = phase;
  }

}
