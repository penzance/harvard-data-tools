package edu.harvard.data.generator;

import edu.harvard.data.schema.DataSchema;

public class SchemaPhase {

  private DataSchema schema;
  private String prefix;
  private String javaBindingPackage;
  private String hdfsDir;

  public void setJavaBindingPackage(final String javaBindingPackage) {
    this.javaBindingPackage = javaBindingPackage;
  }

  public void setPrefix(final String prefix) {
    this.prefix = prefix;
  }

  public void setSchema(final DataSchema schema) {
    this.schema = schema;
  }

  public String getJavaBindingPackage() {
    return javaBindingPackage;
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
