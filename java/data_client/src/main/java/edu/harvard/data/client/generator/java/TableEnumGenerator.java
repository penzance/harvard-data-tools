package edu.harvard.data.client.generator.java;

import java.io.PrintStream;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.client.generator.SchemaPhase;

public class TableEnumGenerator {

  private static final Logger log = LogManager.getLogger();

  private final SchemaPhase tableVersion;
  private final List<String> tableNames;
  private final String schemaVersion;

  public TableEnumGenerator(final String schemaVersion, final List<String> tableNames,
      final SchemaPhase tableVersion) {
    this.schemaVersion = schemaVersion;
    this.tableNames = tableNames;
    this.tableVersion = tableVersion;
  }

  public void generate(final PrintStream out) {
    final String classPrefix = tableVersion.getPrefix();
    final String baseEnumName = tableVersion.getTableEnumName();
    log.info("Generating Table Enum");
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);
    out.println("package " + tableVersion.getJavaPackage() + ";");
    out.println();
    out.println("import " + tableVersion.getClientPackage() + ".DataTable;");
    out.println();
    out.println("public enum " + classPrefix + baseEnumName + " {");
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
    out.println("  private " + classPrefix + baseEnumName +
        "(final String sourceName, Class<? extends DataTable> tableClass) {");
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
        "  public static " + classPrefix + baseEnumName + " fromSourceName(String sourceName) {");
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
