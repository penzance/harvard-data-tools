package edu.harvard.data.data_tool_generator.java;

import java.io.PrintStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFormat;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaColumn;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaTable;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaType;
import edu.harvard.data.data_tool_generator.SchemaPhase;

// Generate a single model class. A model may have a previous version if the model
// belongs to the second step in the processing pipeline or later. For example, class
// ExtendedRequests will have a previous version Requests. If there is a previous version,
// we generate a constructor to convert from the previous to the current version of the
// type.
//
// We generate get methods for all fields in the model, but only set methods for fields
// that have been added in the current processing step. The reasoning is that we should
// be adding data in the procesing pipeline, not modifying existing fields. If we ever
// want to change that assumption, see the code in outputGettersAndSetters.
public class ModelClassGenerator {

  private static final Logger log = LogManager.getLogger();

  private final String schemaVersion;
  private final CanvasDataSchemaTable table;
  private final SchemaPhase tableVersion;
  private final SchemaPhase previousVersion;
  private final String className;
  private final String previousClassName;

  public ModelClassGenerator(final String schemaVersion, final SchemaPhase tableVersion,
      final SchemaPhase previousVersion, final CanvasDataSchemaTable table) {
    this.schemaVersion = schemaVersion;
    this.table = table;
    this.tableVersion = tableVersion;
    this.previousVersion = previousVersion;
    final String classPrefix = tableVersion.getPrefix();
    this.className = JavaBindingGenerator.javaClass(table.getTableName(), classPrefix);
    this.previousClassName = previousVersion == null ? null
        : JavaBindingGenerator.javaClass(table.getTableName(), previousVersion.getPrefix());
  }

  public void generate(final PrintStream out) {
    log.info("Generating table " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);

    out.println("package " + tableVersion.getJavaPackage() + ";");
    out.println();

    outputImportStatements(out);
    out.println("public class " + className + " implements DataTable {");
    outputFields(out);
    outputCsvConstructor(out);
    outputPreviousClassConstructor(out);
    outputAllFieldConstructor(out);
    outputGettersAndSetters(out);
    outputGetFieldsAsListMethod(out);
    outputGetFieldNames(out);
    out.println("}");
  }

  // Generate the import statements required for the class. Only produce imports
  // for classes that are actually used (to avoid compiler warnings).
  private void outputImportStatements(final PrintStream out) {
    if (hasTimestampColumn(table)) {
      out.println("import java.sql.Timestamp;");
    }
    if (hasDateColumn(table)) {
      out.println("import java.util.Date;");
      out.println("import java.text.ParseException;");
    }
    out.println("import java.util.ArrayList;");
    out.println("import java.util.List;");
    out.println();
    out.println("import org.apache.commons.csv.CSVRecord;");
    out.println("import " + DataTable.class.getName() + ";");
    out.println("import " + TableFormat.class.getName() + ";");
    out.println();
    if (previousVersion != null && !table.getNewGenerated()) {
      out.println("import " + previousVersion.getJavaPackage() + "." + previousClassName + ";");
      out.println();
    }
  }

  // Generate the field declarations
  private void outputFields(final PrintStream out) {
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      final String typeName = JavaBindingGenerator.javaType(column.getType());
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      out.println("  private " + typeName + " " + variableName + ";");
    }
    out.println();
  }

  // Generate a constructor that takes the TableFormat and a CSVRecord. This
  // constructor parses the CSV record; the TableFormat is required to parse any
  // dates that appear in the table.
  private void outputCsvConstructor(final PrintStream out) {
    if (hasDateColumn(table)) {
      out.println("  public " + className
          + "(final TableFormat format, final CSVRecord record) throws ParseException {");
    } else {
      out.println("  public " + className + "(final TableFormat format, final CSVRecord record) {");
    }
    int columnIdx = 0;
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      generateParseFromCsv(out, column, columnIdx++);
    }
    out.println("  }");
    out.println();
  }

  // Generate a constructor to go from a previous phase of the table (if one
  // exists). For example, class ExtendedRequests will have a constructor
  // 'public ExtendedRequests(Requests requests)'
  private void outputPreviousClassConstructor(final PrintStream out) {
    if (previousClassName != null && !table.getNewGenerated()) {
      final String previousVar = JavaBindingGenerator.javaVariable(previousClassName);
      out.println("  public " + className + "(" + previousClassName + " " + previousVar + ") {");
      for (final CanvasDataSchemaColumn column : table.getColumns()) {
        if (!column.getNewGenerated()) {
          final String variableName = JavaBindingGenerator.javaVariable(column.getName());
          final String methodName = "get" + JavaBindingGenerator.javaClass(variableName, "");
          out.println("    this." + variableName + " = " + previousVar + "." + methodName + "();");
        }
      }
      out.println("  }");
      out.println();
    }
  }

  // Generate a constructor that takes each field in order as parameters.
  private void outputAllFieldConstructor(final PrintStream out) {
    out.println("  public " + className + "(");
    int columnCount = 0;
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      final String paramType = JavaBindingGenerator.javaType(column.getType());
      final String paramName = JavaBindingGenerator.javaVariable(column.getName());
      out.print("        " + paramType + " " + paramName);
      if (++columnCount == table.getColumns().size()) {
        out.println(") {");
      } else {
        out.println(",");
      }
    }
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      out.println("    this." + variableName + " = " + variableName + ";");
    }
    out.println("  }");
    out.println();
  }

  // Generate getters for each field, and setters for any fields that have been
  // added in this step of processing.
  private void outputGettersAndSetters(final PrintStream out) {
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      final String typeName = JavaBindingGenerator.javaType(column.getType());
      String methodName = "get" + JavaBindingGenerator.javaClass(column.getName(), "");
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      JavaBindingGenerator.writeComment(column.getDescription(), 2, out, true);
      out.println("  public " + typeName + " " + methodName + "() {");
      out.println("    return this." + variableName + ";");
      out.println("  }");
      out.println();
      // Generate setters only for new fields
      if (column.getNewGenerated()) {
        methodName = "set" + JavaBindingGenerator.javaClass(column.getName(), "");
        JavaBindingGenerator.writeComment(column.getDescription(), 2, out, true);
        out.println("  public void " + methodName + "(" + typeName + " " + variableName + ") {");
        out.println("    this." + variableName + " = " + variableName + ";");
        out.println("  }");
        out.println();
      }
    }
  }

  // Generate an implementation of the getFieldsAsList method. This method
  // returns an ArrayList containing each field in the order in which they were
  // defined in the schema.
  private void outputGetFieldsAsListMethod(final PrintStream out) {
    out.println("  @Override");
    out.println("  public List<Object> getFieldsAsList(final TableFormat formatter) {");
    out.println("    final List<Object> fields = new ArrayList<Object>();");
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      if (isTimestamp(column) || isDate(column)) {
        out.println("    fields.add(formatter.formatTimestamp(" + variableName + "));");
      } else {
        out.println("    fields.add(" + variableName + ");");
      }
    }
    out.println("    return fields;");
    out.println("  }");
  }

  // Generate a method that returns an ArrayList of Strings, holding the field
  // names in the order that they were defined in the schema.
  private void outputGetFieldNames(final PrintStream out) {
    out.println();
    out.println("  public static List<String> getFieldNames() {");
    out.println("    final List<String> fields = new ArrayList<String>();");
    for (final CanvasDataSchemaColumn column : table.getColumns()) {
      out.println("      fields.add(\"" + column.getName() + "\");");
    }
    out.println("    return fields;");
    out.println("  }");
  }

  private boolean isTimestamp(final CanvasDataSchemaColumn c) {
    return (c.getType() == CanvasDataSchemaType.Timestamp
        || c.getType() == CanvasDataSchemaType.DateTime);
  }

  private boolean isDate(final CanvasDataSchemaColumn c) {
    return c.getType() == CanvasDataSchemaType.Date;
  }

  // Checks the whole table for any column that is of type Date
  private boolean hasDateColumn(final CanvasDataSchemaTable table) {
    for (final CanvasDataSchemaColumn c : table.getColumns()) {
      if (isDate(c)) {
        return true;
      }
    }
    return false;
  }

  // Checks the whole table for any column that is of type Timestamp
  private boolean hasTimestampColumn(final CanvasDataSchemaTable table) {
    for (final CanvasDataSchemaColumn c : table.getColumns()) {
      if (isTimestamp(c)) {
        return true;
      }
    }
    return false;
  }

  // Determine the code needed to parse a value from the CSV reader.
  // The CSV reader returns all data as Strings, so we must use the appropriate
  // valueOf method in the case of boxed primitive types, or use the TableFormat
  // class to parse dates and timestamps.
  private void generateParseFromCsv(final PrintStream out, final CanvasDataSchemaColumn column,
      final int idx) {
    String parseMethod = null;
    final String extraParams = "";
    switch (column.getType()) {
    case BigInt:
      parseMethod = "Long.valueOf";
      break;
    case Boolean:
      parseMethod = "Boolean.valueOf";
      break;
    case DateTime:
    case Timestamp:
      parseMethod = "Timestamp.valueOf";
      break;
    case Date:
      parseMethod = "format.getDateFormat().parse";
      break;
    case DoublePrecision:
      parseMethod = "Double.valueOf";
      break;
    case Int:
    case Integer:
      parseMethod = "Integer.valueOf";
      break;
    case Text:
    case VarChar:
      break;
    default:
      throw new RuntimeException("Unknown data type: " + column.getType());
    }
    final String getRecord = "record.get(" + idx + ")";
    final String varName = JavaBindingGenerator.javaVariable(column.getName());
    if (parseMethod == null) {
      out.println("    this." + varName + " = " + getRecord + ";");
    } else {
      final String tmpName = "$" + varName;
      out.println("    String " + tmpName + " = " + getRecord + ";");
      out.println("    if (" + tmpName + " != null && " + tmpName + ".length() > 0) {");
      out.println(
          "      this." + varName + " = " + parseMethod + "(" + tmpName + extraParams + ");");
      out.println("    }");
    }
  }

}
