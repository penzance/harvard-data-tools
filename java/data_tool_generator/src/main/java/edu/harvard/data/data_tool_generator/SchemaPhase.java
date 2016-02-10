package edu.harvard.data.data_tool_generator;

import java.io.File;

import edu.harvard.data.client.canvas.api.CanvasDataSchema;

public class SchemaPhase {

  private CanvasDataSchema schema;
  private String prefix;

  private String javaPackage;
  private File javaSourceLocation;
  private String hdfsDir;

  public void setJavaSourceDir(final File sourceDir) {
    this.javaSourceLocation = sourceDir;
  }

  public void setJavaPackage(final String javaPackage) {
    this.javaPackage = javaPackage;
  }

  public void setPrefix(final String prefix) {
    this.prefix = prefix;
  }

  public void setSchema(final CanvasDataSchema schema) {
    this.schema = schema;
  }

  public String getJavaPackage() {
    return javaPackage;
  }

  public String getPrefix() {
    return prefix;
  }

  public File getJavaSourceLocation() {
    return javaSourceLocation;
  }

  public CanvasDataSchema getSchema() {
    return schema;
  }

  public void setHDFSDir(final String dir) {
    this.hdfsDir = dir;
  }

  public String getHDFSDir() {
    return hdfsDir;
  }
}
