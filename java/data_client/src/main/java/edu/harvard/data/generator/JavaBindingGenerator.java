package edu.harvard.data.generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;

public class JavaBindingGenerator {

  private static final Logger log = LogManager.getLogger();

  private static final String POM_XML_TEMPLATE = "pom.xml.template";

  private final GenerationSpec spec;
  private final GenerationSpec schemaVersions;
  private final String projectName;
  private final File javaSrcBase;
  private final File baseDir;

  public JavaBindingGenerator(final GenerationSpec spec) {
    this.spec = spec;
    baseDir = new File(spec.getOutputBase(), "java");
    this.javaSrcBase = new File(baseDir, "src/main/java");
    this.schemaVersions = spec;
    this.projectName = spec.getJavaProjectName();
  }

  // Generates a new Maven project in the directory passed to the constructor.
  // The project has a pom.xml file and three sets of bindings (one for each
  // stage of data processing):
  //
  // Phase Zero bindings are generated from the JSON schema provided by
  // Instructure (passed to the class constructor).
  //
  // Phase One bindings are produced by the first EMR job which supplements the
  // existing data set with new calculated data. The new tables and fields are
  // specified in PHASE_ONE_ADDITIONS_JSON.
  //
  // Phase Two bindings are produced by the second EMR job, and result from the
  // merging of multiple data sets. The new tables and fields are specified in
  // PHASE_TWO_ADDITIONS_JSON.
  //
  public void generate() throws IOException {
    javaSrcBase.mkdirs();

    // Create the pom.xml file from a template in src/main/resources.
    copyPomXml();

    // Generate bindings for each step in the processing pipeline.
    generateTableSet(0, schemaVersions.getPhase(0), null);
    generateTableSet(1, schemaVersions.getPhase(1), schemaVersions.getPhase(0));
    generateTableSet(2, schemaVersions.getPhase(2), schemaVersions.getPhase(1));
    generateTableSet(3, schemaVersions.getPhase(3), schemaVersions.getPhase(2));
  }

  // Generate the bindings for one step in the processing pipeline. There are
  // three generators used:
  //
  // TableGenerator creates the Table enum type with a constant for each table.
  //
  // TableFactoryGenerator creates the TableFactory subtype that lets us create
  // readers and writers dynamically.
  //
  // TableGenerator is run once per table, and creates the individual table
  // class.
  //
  private void generateTableSet(final int phase, final SchemaPhase currentVersion,
      final SchemaPhase previousVersion) throws IOException {
    final File srcDir = new File(javaSrcBase,
        currentVersion.getJavaBindingPackage().replaceAll("\\.", File.separator));
    final String classPrefix = currentVersion.getPrefix();
    final String version = currentVersion.getSchema().getVersion();
    final Map<String, DataSchemaTable> tables = currentVersion.getSchema().getTables();
    final String tableEnumName = spec.getJavaTableEnumName();

    // Create the base directory where all of the classes will be generated
    log.info("Generating tables in " + srcDir);
    if (srcDir.exists()) {
      log.info("Deleting: " + srcDir);
      FileUtils.deleteDirectory(srcDir);
    }
    srcDir.mkdirs();
    final List<String> tableNames = generateTableNames(phase, tables);

    // Generate the Table enum.
    final File tableEnumFile = new File(srcDir, classPrefix + tableEnumName + ".java");
    try (final PrintStream out = new PrintStream(new FileOutputStream(tableEnumFile))) {
      new JavaTableEnumGenerator(version, tableNames, currentVersion, tableEnumName).generate(out);
    }

    // Generate the TableFactory class.
    final File tableFactoryFile = new File(srcDir, classPrefix + tableEnumName + "Factory.java");
    try (final PrintStream out = new PrintStream(new FileOutputStream(tableFactoryFile))) {
      new JavaTableFactoryGenerator(version, tableNames, currentVersion, tableEnumName).generate(out);
    }

    // Generate a model class for each table.
    for (final String name : tables.keySet()) {
      final DataSchemaTable table = tables.get(name);
      if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
        final String className = javaClass(table.getTableName(), classPrefix);
        final File classFile = new File(srcDir, className + ".java");
        try (final PrintStream out = new PrintStream(new FileOutputStream(classFile))) {
          new JavaModelClassGenerator(version, currentVersion, previousVersion, table).generate(out);
        }
      }
    }
  }

  // Generate the pom.xml file for the Maven project, based off a template in
  // the src/main/resources directory.
  private void copyPomXml() throws IOException {
    final File pomFile = new File(baseDir, "pom.xml");
    log.info("Creating pom.xml file at " + pomFile);
    try (
        InputStream inStream = this.getClass().getClassLoader()
        .getResourceAsStream(POM_XML_TEMPLATE);
        BufferedReader in = new BufferedReader(new InputStreamReader(inStream));
        BufferedWriter out = new BufferedWriter(new FileWriter(pomFile))) {
      String line = in.readLine();
      while (line != null) {
        // Replace the $artifact_id variable in the template.
        out.write(line.replaceAll("\\$artifact_id", projectName) + "\n");
        line = in.readLine();
      }
    }
  }

  // Generate a sorted list of table names for the switch tables in the enum and
  // factory classes
  static List<String> generateTableNames(final int phase, final Map<String, DataSchemaTable> tables) {
    final List<String> tableNames = new ArrayList<String>();
    for (final String name : tables.keySet()) {
      final DataSchemaTable table = tables.get(name);
      if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
        tableNames.add(table.getTableName());
      }
    }
    Collections.sort(tableNames);
    return tableNames;
  }

  // Write a standard file header to warn future developers against editing the
  // generated files.
  public static void writeFileHeader(final PrintStream out, final String version) {
    writeComment("This file was generated automatically. Do not edit.",
        0, out, false);
    out.println();
  }

  // Output a comment string, propery formatted. Uses the double-slash format
  // unless 'javadoc' is set, in which case it will use the /**...*/ format.
  static void writeComment(final String text, final int indent, final PrintStream out,
      final boolean javadoc) {
    if (text == null) {
      return;
    }
    if (javadoc) {
      writeIndent(indent, out);
      out.println("/**");
    }
    final int maxLine = 80;
    startNewCommentLine(indent, out, javadoc);
    int currentLine = indent + 3;
    for (final String word : text.split(" ")) {
      currentLine += word.length() + 1;
      if (currentLine > maxLine) {
        out.println();
        startNewCommentLine(indent, out, javadoc);
        currentLine = indent + 3 + word.length();
      }
      out.print(word + " ");
    }
    if (javadoc) {
      out.println();
      writeIndent(indent, out);
      out.print(" */");
    }
    out.println();
  }

  // Helper to indent comments properly
  static void writeIndent(final int indent, final PrintStream out) {
    for (int i = 0; i < indent; i++) {
      out.print(" ");
    }
  }

  static int startNewCommentLine(final int indent, final PrintStream out, final boolean javadoc) {
    writeIndent(indent, out);
    if (javadoc) {
      out.print(" * ");
      return 2;
    } else {
      out.print("// ");
      return 3;
    }
  }

  public static String javaEnum(final DataSchemaColumn column) {
    String columnName = column.getName();
    if (columnName.contains(".")) {
      columnName = columnName.substring(columnName.lastIndexOf(".") + 1);
    }
    return javaClass(columnName + "Enum", "");
  }

  // Format a String into the CorrectJavaClassName format.
  public static String javaClass(final String str, final String classPrefix) {
    String className = classPrefix;
    for (final String part : str.split("_")) {
      if (part.length() > 0) {
        className += part.substring(0, 1).toUpperCase()
            + (part.length() > 1 ? part.substring(1) : "");
      }
    }
    return className;
  }

  // Borrow the javaClass method to easily produce properly-formatted getters
  // and setters.
  public static String javaGetter(final String fieldName) {
    String name = fieldName;
    if (name.contains(".")) {
      name = name.substring(name.lastIndexOf(".") + 1);
    }
    return javaClass(name, "get");
  }

  public static String javaSetter(final String fieldName) {
    String name = fieldName;
    if (name.contains(".")) {
      name = name.substring(name.lastIndexOf(".") + 1);
    }
    return javaClass(name, "set");
  }

  // Format a String into the correctJavaVariableName format.
  public static String javaVariable(final String name) {
    String fieldName = name;
    // Dots are used in the schema to indicate nested JSON values
    if (fieldName.contains(".")) {
      fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);
    }
    final String[] parts = fieldName.split("_");
    String variableName = parts[0].substring(0, 1).toLowerCase() + parts[0].substring(1);
    for (int i = 1; i < parts.length; i++) {
      final String part = parts[i];
      variableName += part.substring(0, 1).toUpperCase() + part.substring(1);
    }
    if (variableName.equals("public")) {
      variableName = "_public";
    }
    if (variableName.equals("default")) {
      variableName = "_default";
    }
    return variableName;
  }

  // Convert the types specified in the schema.json format into Java types.
  public static String javaType(final DataSchemaColumn column) {
    switch (column.getType()) {
    case BigInt:
      return Long.class.getSimpleName();
    case Boolean:
      return Boolean.class.getSimpleName();
    case Date:
      return Date.class.getSimpleName();
    case DateTime:
    case Timestamp:
      return Timestamp.class.getSimpleName();
    case DoublePrecision:
      return Double.class.getSimpleName();
    case Integer:
      return Integer.class.getSimpleName();
    case Guid:
    case Text:
    case VarChar:
      return String.class.getSimpleName();
    case Enum:
      return javaEnum(column);
    }
    throw new RuntimeException("Unknown data type: " + column.getType());
  }

}
