package edu.harvard.data.generator;

import edu.harvard.data.schema.DataSchema;

public class SchemaVersion {

  private final DataSchema schema;
  private final String prefix;
  private final String bindingPackage;

  public SchemaVersion(final DataSchema schema, final String packageBase, final int phase) {
    this.schema = schema;
    this.prefix = "Phase" + phase;
    this.bindingPackage = packageBase + ".bindings.phase_" + phase;
  }

  public String getJavaBindingPackage() {
    return bindingPackage;
  }

  public String getPrefix() {
    return prefix;
  }

  public DataSchema getSchema() {
    return schema;
  }

}
