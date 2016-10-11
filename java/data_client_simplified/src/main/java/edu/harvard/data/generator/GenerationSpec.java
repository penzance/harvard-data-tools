package edu.harvard.data.generator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;

/**
 * This class defines the main interface between the generator and any client
 * code. The {@code GenerationSpec} class has two main purposes. It gathers the
 * parameters and settings required to correctly generate a set of Java, Bash
 * and SQL code for a given data set.
 */
public class GenerationSpec {

  private static final int SCHEMA_VERSIONS = 3;

  private final List<SchemaPhase> phases;
  private File outputBase;
  private String tableEnumName;
  private String identityPackage;
  private IdentifierType mainIdentifier;
  private String javaProjectName;
  private DataConfig config;
  private final String schemaVersion;

  public GenerationSpec(final String schemaVersion) {
    this.schemaVersion = schemaVersion;
    this.phases = new ArrayList<SchemaPhase>();
    for (int i = 0; i < SCHEMA_VERSIONS; i++) {
      phases.add(new SchemaPhase());
    }
  }

  public SchemaPhase getPhase(final int i) {
    return phases.get(i);
  }

  public List<SchemaPhase> getSchemaPhases() {
    return phases;
  }

  public File getOutputBase() {
    return outputBase;
  }

  public String getJavaTableEnumName() {
    return tableEnumName;
  }

  public SchemaPhase getLastPhase() {
    return phases.get(phases.size() - 1);
  }

  public String getIdentityPackage() {
    return identityPackage;
  }

  public void setOutputBaseDirectory(final File outputBase) {
    this.outputBase = outputBase;
  }

  public void setIdentityPackage(final String identityPackage) {
    this.identityPackage = identityPackage;
  }

  public void setMainIdentifier(final IdentifierType mainIdentifier) {
    this.mainIdentifier = mainIdentifier;
  }

  public IdentifierType getMainIdentifier() {
    return mainIdentifier;
  }

  public void setJavaProjectName(final String javaProjectName) {
    this.javaProjectName = javaProjectName;
  }

  public String getJavaProjectName() {
    return javaProjectName;
  }

  public void setConfig(final DataConfig config) {
    this.config = config;
  }

  public DataConfig getConfig() {
    return config;
  }

  public void setJavaBindingPackages(final String... packageList) {
    if (packageList.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " packages, got " + packageList.length);
    }
    for (int i = 0; i < packageList.length; i++) {
      phases.get(i).setJavaBindingPackage(packageList[i]);
    }
  }

  public void setSchemas(final DataSchema... schemas) {
    if (schemas.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " schemas, got " + schemas.length);
    }
    for (int i = 0; i < schemas.length; i++) {
      phases.get(i).setSchema(schemas[i]);
    }
  }

  public void setPrefixes(final String... prefixes) {
    if (prefixes.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " class prefixes, got " + prefixes.length);
    }
    for (int i = 0; i < prefixes.length; i++) {
      phases.get(i).setPrefix(prefixes[i]);
    }
  }

  public void setJavaTableEnumName(final String tableEnumName) {
    this.tableEnumName = tableEnumName;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }
}
