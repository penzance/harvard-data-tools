package edu.harvard.data.generator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;

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
public class JavaModelClassGenerator {

  private static final Logger log = LogManager.getLogger();

  private final String schemaVersion;
  private final DataSchemaTable table;
  private final SchemaPhase tableVersion;
  private final SchemaPhase previousVersion;
  private final String className;
  private final String previousClassName;
  private final String classPrefix;

  public JavaModelClassGenerator(final String schemaVersion, final SchemaPhase tableVersion,
      final SchemaPhase previousVersion, final DataSchemaTable table) {
    this.schemaVersion = schemaVersion;
    this.table = table;
    this.tableVersion = tableVersion;
    this.previousVersion = previousVersion;
    classPrefix = tableVersion.getPrefix();
    this.className = JavaBindingGenerator.javaClass(table.getTableName(), classPrefix);
    this.previousClassName = previousVersion == null ? null
        : JavaBindingGenerator.javaClass(table.getTableName(), previousVersion.getPrefix());
  }

  public void generate(final PrintStream out) {
    log.info("Generating table " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);

    out.println("package " + tableVersion.getJavaBindingPackage() + ";");
    out.println();

    outputImportStatements(out);
    out.println("public class " + className + " implements DataTable {");
    out.println();
    outputEnumTypes(out);
    outputFields(out);
    outputCsvConstructor(out);
    outputMapConstructor(out);
    outputPreviousClassConstructor(out);
    outputLikeClassConstructor(out);
    outputAllFieldConstructor(out);
    outputGettersAndSetters(out);
    outputGetFieldNames(out);
    outputGetFieldsAsListMethod(out);
    outputGetFieldsAsMapMethod(out);
    out.println("}");
  }

  private void outputEnumTypes(final PrintStream out) {
    for (final DataSchemaColumn column : table.getColumns()) {
      if (column.getType() == DataSchemaType.Enum) {
        final String enumName = JavaBindingGenerator.javaEnum(column);
        out.println("  public enum " + enumName + " {");
        final String desc = column.getDescription();
        final String[] split = desc.split("'");
        for (int i = 1; i < split.length; i += 2) {
          out.print(
              "    " + JavaBindingGenerator.javaClass(split[i], "") + "(\"" + split[i] + "\")");
          if (i + 2 < split.length) {
            out.println(",");
          } else {
            out.println(";");
          }
        }
        out.println();
        out.println("    private static final Map<String, " + enumName + "> values;");
        out.println("    static {");
        out.println("      values = new HashMap<String, " + enumName + ">();");
        out.println("      for (final " + enumName + " v : " + enumName + ".values()) {");
        out.println("        values.put(v.value, v);");
        out.println("      }");
        out.println("    };");
        out.println("    public static " + enumName + " parse(final String str) {");
        out.println("      if (values.containsKey(str)) {");
        out.println("        return values.get(str);");
        out.println("      }");
        out.println("      return valueOf(str);");
        out.println("    }");
        out.println();
        out.println("    private final String value;");
        out.println("    private " + enumName + "(final String value) {");
        out.println("      this.value = value;");
        out.println("    }");
        out.println("    public String getValue() {");
        out.println("      return value;");
        out.println("    }");
        out.println("    @Override");
        out.println("    public String toString() {");
        out.println("      return value;");
        out.println("    }");
        out.println("  }");
        out.println();
      }
    }
  }

  // Generate the import statements required for the class. Only produce imports
  // for classes that are actually used (to avoid compiler warnings).
  private void outputImportStatements(final PrintStream out) {
    if (hasTimestampColumn(table)) {
      out.println("import java.sql.Timestamp;");
    }
    if (hasDateColumn(table) || hasTimestampColumn(table)) {
      out.println("import java.text.ParseException;");
    }
    if (hasDateColumn(table)) {
      out.println("import java.util.Date;");
    }
    out.println("import " + ArrayList.class.getName() + ";");
    out.println("import " + List.class.getName() + ";");
    out.println("import " + Map.class.getName() + ";");
    out.println("import " + HashMap.class.getName() + ";");
    out.println();
    out.println("import org.apache.commons.csv.CSVRecord;");
    out.println("import " + DataTable.class.getName() + ";");
    out.println("import " + TableFormat.class.getName() + ";");
    out.println();
    if (previousVersion != null && !table.getNewlyGenerated()) {
      out.println(
          "import " + previousVersion.getJavaBindingPackage() + "." + previousClassName + ";");
      out.println();
    }
  }

  // Generate the field declarations
  private void outputFields(final PrintStream out) {
    for (final DataSchemaColumn column : table.getColumns()) {
      final String typeName = JavaBindingGenerator.javaType(column);
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
    for (final DataSchemaColumn column : table.getColumns()) {
      outputParseFromString(out, column, "record.get(" + columnIdx + ")");
      columnIdx++;
    }
    out.println("  }");
    out.println();
  }

  // Generate a constructor that takes the TableFormat and a Map<Object,
  // String>. This constructor is required to parse JSON objects. It recognizes
  // nested maps, flattening structures according to the column definitions in
  // the schema description.
  private void outputMapConstructor(final PrintStream out) {
    if (!getNestedMaps().isEmpty()) {
      out.println("  @SuppressWarnings(\"unchecked\")");
    }
    if (hasTimestampColumn(table)) {
      out.println("  public " + className
          + "(final TableFormat format, final Map<String, Object> map) throws ParseException {");
    } else {
      out.println(
          "  public " + className + "(final TableFormat format, Map<String, Object> map) {");
    }
    for (final String map : getNestedMaps()) {
      final String mapName = JavaBindingGenerator.javaVariable(map);
      out.println("    Map<String, Object> " + mapName + " = (Map<String, Object>) map.get(\""
          + mapName + "\");");
    }
    for (final DataSchemaColumn column : table.getColumns()) {
      final String columnName = column.getName();
      final String key;
      final String getMethod;
      final String mapName;
      if (columnName.contains(".")) {
        mapName = JavaBindingGenerator
            .javaVariable(columnName.substring(0, columnName.lastIndexOf(".")));
        key = columnName.substring(columnName.indexOf(".") + 1);
      } else {
        mapName = "map";
        key = columnName;
      }
      getMethod = mapName + ".get(\"" + key + "\")";
      outputGetFromMap(out, column, getMethod);
    }
    out.println("  }");
    out.println();
  }

  // Generate a constructor to go from a previous phase of the table (if one
  // exists). For example, class ExtendedRequests will have a constructor
  // 'public ExtendedRequests(Requests requests)'
  private void outputPreviousClassConstructor(final PrintStream out) {
    if (previousClassName != null && !table.getNewlyGenerated()) {
      final String previousVar = JavaBindingGenerator.javaVariable(previousClassName);
      out.println("  public " + className + "(" + previousClassName + " " + previousVar + ") {");
      for (final DataSchemaColumn column : table.getColumns()) {
        if (!column.getNewlyGenerated()) {
          assignField(out, column, previousVar);
        }
      }
      out.println("  }");
      out.println();
    }
  }

  // If the table was declared as "like" another, include a constructor for that
  // table's model class.
  private void outputLikeClassConstructor(final PrintStream out) {
    if (table.getLikeTable() != null) {
      final String likeTableName = table.getLikeTable();
      final DataSchemaTable likeTable = tableVersion.getSchema().getTableByName(likeTableName);
      final String likeTableClass = JavaBindingGenerator.javaClass(likeTableName, classPrefix);
      out.println("  public " + className + "(" + likeTableClass + " likeTable) {");
      for (final DataSchemaColumn column : likeTable.getColumns()) {
        assignField(out, column, "likeTable");
      }
      out.println("  }");
      out.println();
    }
  }

  // Generate code to assign a value from the get method on another variable,
  // e.g.
  // this.variableName = originalField.getVariableName();
  private void assignField(final PrintStream out, final DataSchemaColumn column,
      final String originalField) {
    final String variableName = JavaBindingGenerator.javaVariable(column.getName());
    final String methodName = "get" + JavaBindingGenerator.javaClass(variableName, "");
    if (column.getType() == DataSchemaType.Enum) {
      final String enumName = JavaBindingGenerator.javaEnum(column);
      out.println("    if (" + originalField + "." + methodName + "() != null) {");
      out.println("      this." + variableName + " = " + enumName + ".parse(" + originalField + "."
          + methodName + "().getValue());");
      out.println("    }");
    } else {
      out.println("    this." + variableName + " = " + originalField + "." + methodName + "();");
    }

  }

  // Generate a constructor that takes each field in order as parameters.
  private void outputAllFieldConstructor(final PrintStream out) {
    out.println("  public " + className + "(");
    int columnCount = 0;
    for (final DataSchemaColumn column : table.getColumns()) {
      final String paramType = JavaBindingGenerator.javaType(column);
      final String paramName = JavaBindingGenerator.javaVariable(column.getName());
      out.print("        " + paramType + " " + paramName);
      if (++columnCount == table.getColumns().size()) {
        out.println(") {");
      } else {
        out.println(",");
      }
    }
    for (final DataSchemaColumn column : table.getColumns()) {
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      out.println("    this." + variableName + " = " + variableName + ";");
    }
    out.println("  }");
    out.println();
  }

  // Generate getters and setters for each field.
  private void outputGettersAndSetters(final PrintStream out) {
    for (final DataSchemaColumn column : table.getColumns()) {
      final String typeName = JavaBindingGenerator.javaType(column);
      String methodName = JavaBindingGenerator.javaGetter(column.getName());
      final String variableName = JavaBindingGenerator.javaVariable(column.getName());
      JavaBindingGenerator.writeComment(column.getDescription(), 2, out, true);
      out.println("  public " + typeName + " " + methodName + "() {");
      out.println("    return this." + variableName + ";");
      out.println("  }");
      out.println();
      methodName = JavaBindingGenerator.javaSetter(column.getName());
      JavaBindingGenerator.writeComment(column.getDescription(), 2, out, true);
      out.println("  public void " + methodName + "(" + typeName + " " + variableName + ") {");
      out.println("    this." + variableName + " = " + variableName + ";");
      out.println("  }");
      out.println();
    }
  }

  // Generate an implementation of the getFieldsAsList method. This method
  // returns an ArrayList containing each field in the order in which they were
  // defined in the schema.
  private void outputGetFieldsAsListMethod(final PrintStream out) {
    out.println("  @Override");
    out.println("  public List<Object> getFieldsAsList(final TableFormat formatter) {");
    out.println("    final List<Object> fields = new ArrayList<Object>();");
    for (final DataSchemaColumn column : table.getColumns()) {
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

  // Generate an implementation of the getFieldsAsMap method. This method
  // returns a HashMap containing each field name and its value. Any nested
  // object (those described in the schema as outer.inner) will be represented
  // as an inner map.
  private void outputGetFieldsAsMapMethod(final PrintStream out) {
    out.println("  @Override");
    out.println("  public Map<String, Object> getFieldsAsMap() {");
    out.println("    Map<String, Object> $map = new HashMap<String, Object>();");
    // Add any nested objects as new maps
    for (final String map : getNestedMaps()) {
      final String mapName = JavaBindingGenerator.javaVariable(map);
      out.println("    Map<String, Object> $" + mapName + " = new HashMap<String, Object>();");
      out.println("    $map.put(\"" + mapName + "\", $" + mapName + ");");
    }
    for (final DataSchemaColumn column : table.getColumns()) {
      final String columnName = column.getName();
      final String mapName;
      final String key;
      final String variableName;
      if (columnName.contains(".")) {
        mapName = JavaBindingGenerator
            .javaVariable(columnName.substring(0, columnName.lastIndexOf(".")));
        key = columnName.substring(columnName.lastIndexOf(".") + 1);
        variableName = JavaBindingGenerator.javaVariable(key);
      } else {
        mapName = "map";
        key = columnName;
        variableName = JavaBindingGenerator.javaVariable(columnName);
      }
      out.println("    $" + mapName + ".put(\"" + key + "\", " + variableName + ");");
    }
    out.println("    return $map;");
    out.println("  }");
  }

  private List<String> getNestedMaps() {
    final Set<String> maps = new HashSet<String>();
    for (final DataSchemaColumn column : table.getColumns()) {
      final String columnName = column.getName();
      if (columnName.contains(".")) {
        maps.add(columnName.substring(0, columnName.lastIndexOf(".")));
      }
    }
    final List<String> mapList = new ArrayList<String>(maps);
    Collections.sort(mapList);
    return mapList;
  }

  // Generate a method that returns an ArrayList of Strings, holding the field
  // names in the order that they were defined in the schema.
  private void outputGetFieldNames(final PrintStream out) {
    out.println();
    out.println("  @Override");
    out.println("  public List<String> getFieldNames() {");
    out.println("    final List<String> fields = new ArrayList<String>();");
    for (final DataSchemaColumn column : table.getColumns()) {
      out.println("      fields.add(\"" + column.getName() + "\");");
    }
    out.println("    return fields;");
    out.println("  }");
  }

  private boolean isTimestamp(final DataSchemaColumn c) {
    return (c.getType() == DataSchemaType.Timestamp || c.getType() == DataSchemaType.DateTime);
  }

  private boolean isDate(final DataSchemaColumn c) {
    return c.getType() == DataSchemaType.Date;
  }

  // Checks the whole table for any column that is of type Date
  private boolean hasDateColumn(final DataSchemaTable table) {
    for (final DataSchemaColumn c : table.getColumns()) {
      if (isDate(c)) {
        return true;
      }
    }
    return false;
  }

  // Checks the whole table for any column that is of type Enum
  private boolean hasEnumColumn(final DataSchemaTable table) {
    for (final DataSchemaColumn c : table.getColumns()) {
      if (c.getType() == DataSchemaType.Enum) {
        return true;
      }
    }
    return false;
  }

  // Checks the whole table for any column that is of type Timestamp
  private boolean hasTimestampColumn(final DataSchemaTable table) {
    for (final DataSchemaColumn c : table.getColumns()) {
      if (isTimestamp(c)) {
        return true;
      }
    }
    return false;
  }

  // Determine the code needed to correctly type a value extracted from an
  // untyped map.
  private void outputGetFromMap(final PrintStream out, final DataSchemaColumn column,
      final String getMethod) {
    final String variableName = JavaBindingGenerator.javaVariable(column.getName());
    switch (column.getType()) {
    case BigInt:
      out.println("    if (" + getMethod + " instanceof Integer) {");
      out.println("      this." + variableName + " = Long.parseLong(((Integer) " + getMethod
          + ").toString());");
      out.println("    } else {");
      out.println("      this." + variableName + " = (Long) " + getMethod + ";");
      out.println("    }");
      break;
    case Boolean:
      out.println("    if (" + getMethod + " instanceof Integer) {");
      out.println("      this." + variableName + " = ((Integer) " + getMethod + ") != 0;");
      out.println("    } else {");
      out.println("      this." + variableName + " = (Boolean) " + getMethod + ";");
      out.println("    }");
      break;
    case Date:
    case DateTime:
    case Timestamp:
      final String tmpName = "$" + variableName;
      out.println("    String " + tmpName + " = (String) " + getMethod + ";");
      out.println("    if (" + tmpName + " != null && " + tmpName + ".length() > 0) {");
      out.println("    this." + variableName + " = new Timestamp(format.getTimstampFormat().parse("
          + tmpName + ").getTime());");
      out.println("    }");
      break;
    case Enum:
      outputParseFromString(out, column, "(String) " + getMethod);
      break;
    case DoublePrecision:
      out.println("    this." + variableName + " = (Double) " + getMethod + ";");
      break;
    case Guid:
    case Text:
    case VarChar:
      out.println("    this." + variableName + " = (String) " + getMethod + ";");
      break;
    case Integer:
      out.println("    this." + variableName + " = (Integer) " + getMethod + ";");
      break;
    }
  }

  // Determine the code needed to parse a value from the CSV reader.
  // The CSV reader returns all data as Strings, so we must use the appropriate
  // valueOf method in the case of boxed primitive types, or use the TableFormat
  // class to parse dates and timestamps.
  private void outputParseFromString(final PrintStream out, final DataSchemaColumn column,
      final String getRecord) {
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
    case Integer:
      parseMethod = "Integer.valueOf";
      break;
    case Enum:
      parseMethod = JavaBindingGenerator.javaEnum(column) + ".parse";
    case Guid:
    case Text:
    case VarChar:
      break;
    }
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
