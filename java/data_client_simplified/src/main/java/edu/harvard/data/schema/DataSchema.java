package edu.harvard.data.schema;

import java.util.List;
import java.util.Map;

public interface DataSchema {

  Map<String, DataSchemaTable> getTables();

  String getVersion();

  DataSchema copy();

  DataSchemaTable getTableByName(String name);

  void addTable(String tableName, DataSchemaTable newTable);

  List<String> getTableNames();

}
