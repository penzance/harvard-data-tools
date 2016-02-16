package edu.harvard.data.client.schema;

import java.util.Map;

public interface DataSchema {

  Map<String, DataSchemaTable> getTables();

  String getVersion();

  DataSchema copy();

}
