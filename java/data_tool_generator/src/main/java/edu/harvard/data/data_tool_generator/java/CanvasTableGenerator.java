package edu.harvard.data.data_tool_generator.java;

import java.io.PrintStream;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.data_tool_generator.CanvasDataGenerator;
import edu.harvard.data.data_tool_generator.SchemaPhase;

public class CanvasTableGenerator {

  private static final Logger log = LogManager.getLogger();

  private final SchemaPhase tableVersion;
  private final List<String> tableNames;
  private final String schemaVersion;

  public CanvasTableGenerator(final String schemaVersion, final List<String> tableNames,
      final SchemaPhase tableVersion) {
    this.schemaVersion = schemaVersion;
    this.tableNames = tableNames;
    this.tableVersion = tableVersion;
  }

  public void generate(final PrintStream out) {
    final String classPrefix = tableVersion.getPrefix();
    log.info("Generating CanvasTable Enum");
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);
    out.println("package " + tableVersion.getJavaPackage() + ";");
    out.println();
    out.println("import " + CanvasDataGenerator.CLIENT_PACKAGE + ".DataTable;");
    out.println();
    out.println("public enum " + classPrefix + "CanvasTable {");
    for (int i = 0; i < tableNames.size(); i++) {
      final String name = tableNames.get(i);
      final String className = JavaBindingGenerator.javaClass(name, classPrefix);
      out.print("  " + className + "(\"" + name + "\", " + className + ".class)");
      out.println(i == (tableNames.size() - 1) ? ";" : ",");
    }
    out.println();
    out.println("  private final String sourceName;");
    out.println("  private final Class<? extends DataTable> tableClass;");
    out.println();
    out.println("  private " + classPrefix
        + "CanvasTable(final String sourceName, Class<? extends DataTable> tableClass) {");
    out.println("    this.sourceName = sourceName;");
    out.println("    this.tableClass = tableClass;");
    out.println("  }");
    out.println();
    out.println("  public String getSourceName() {");
    out.println("    return sourceName;");
    out.println("  }");
    out.println();
    out.println("  public Class<? extends DataTable> getTableClass() {");
    out.println("    return tableClass;");
    out.println("  }");
    out.println();
    out.println(
        "  public static " + classPrefix + "CanvasTable fromSourceName(String sourceName) {");
    out.println("    switch(sourceName) {");
    for (final String name : tableNames) {
      final String className = JavaBindingGenerator.javaClass(name, classPrefix);
      out.println("    case \"" + name + "\": return " + className + ";");
    }
    out.println("    default: throw new RuntimeException(\"Unknown table name \" + sourceName);");
    out.println("    }");
    out.println("  }");
    out.println("}");
  }

}
