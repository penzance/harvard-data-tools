package edu.harvard.data.generator;

import edu.harvard.data.schema.DataSchema;

public class SchemaPhase {

  private DataSchema schema;
  private String prefix;
  private String javaPackage;
  private String hdfsDir;

  public void setJavaPackage(final String javaPackage) {
    this.javaPackage = javaPackage;
  }

  public void setPrefix(final String prefix) {
    this.prefix = prefix;
  }

  public void setSchema(final DataSchema schema) {
    this.schema = schema;
  }

  public String getJavaPackage() {
    return javaPackage;
  }

  public String getPrefix() {
    return prefix;
  }

  public DataSchema getSchema() {
    return schema;
  }

  public void setHDFSDir(final String dir) {
    this.hdfsDir = dir;
  }

  public String getHDFSDir() {
    return hdfsDir;
  }

}
