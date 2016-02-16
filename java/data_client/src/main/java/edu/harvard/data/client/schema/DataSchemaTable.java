package edu.harvard.data.client.schema;

import java.util.List;

public interface DataSchemaTable {

  boolean hasNewlyGeneratedElements();

  String getTableName();

  // Java, Hive, etc
  String getOwner();

  List<DataSchemaColumn> getColumns();

  boolean getNewGenerated();

  void setNewGenerated(boolean flag);

  void setOwner(String owner);

}
