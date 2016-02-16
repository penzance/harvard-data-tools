package edu.harvard.data.client.schema;

import java.util.Map;

public interface DataSchema {

  Map<String, DataSchemaTable> getSchema();

  String getVersion();

  DataSchema copy();

}
