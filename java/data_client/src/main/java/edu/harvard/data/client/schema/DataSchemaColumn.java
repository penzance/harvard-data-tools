package edu.harvard.data.client.schema;

public interface DataSchemaColumn {

  String getName();

  String getHiveType();

  DataSchemaType getType();

  boolean getNewGenerated();

  String getDescription();

  String getRedshiftType();

  void setNewGenerated(boolean flag);

}
