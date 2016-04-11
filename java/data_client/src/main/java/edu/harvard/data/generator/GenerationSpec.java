package edu.harvard.data.generator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.schema.DataSchema;

/**
 * This class defines the main interface between the generator and any client
 * code. The {@code GenerationSpec} class has two main purposes. It gathers the
 * parameters and settings required to correctly generate a set of Java, Bash
 * and SQL code for a given data set.
 */
public class GenerationSpec {

  private final List<SchemaPhase> phases;
  private File outputBase;
  private String tableEnumName;
  private String identityHadoopPackage;

  public GenerationSpec(final int phaseCount) {
    this.phases = new ArrayList<SchemaPhase>();
    for (int i = 0; i < phaseCount; i++) {
      phases.add(new SchemaPhase());
    }
  }

  public SchemaPhase getPhase(final int i) {
    return phases.get(i);
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

  public String getIdentityHadoopPackage() {
    return identityHadoopPackage;
  }

  public void setOutputBaseDirectory(final File outputBase) {
    this.outputBase = outputBase;
  }

  public void setJavaHadoopPackage(final String identityHadoopPackage) {
    this.identityHadoopPackage = identityHadoopPackage;
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

  public void setHdfsDirectories(final String... hdfsDirs) {
    if (hdfsDirs.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " HDFS directories, got " + hdfsDirs.length);
    }
    for (int i = 0; i < hdfsDirs.length; i++) {
      phases.get(i).setHDFSDir(hdfsDirs[i]);
    }
  }
}
