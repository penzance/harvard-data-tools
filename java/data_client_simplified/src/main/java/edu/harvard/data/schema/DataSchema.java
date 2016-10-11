package edu.harvard.data.schema;

import java.util.Map;

public interface DataSchema {

  Map<String, DataSchemaTable> getTables();

  String getVersion();

  DataSchema copy();

  DataSchemaTable getTableByName(String name);

  void addTable(String tableName, DataSchemaTable newTable);

}
