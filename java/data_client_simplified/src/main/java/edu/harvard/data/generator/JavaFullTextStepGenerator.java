package edu.harvard.data.generator;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.CloseableMap;
import edu.harvard.data.DataTable;
import edu.harvard.data.FullTextTable;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.VerificationException;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.fulltext.FullTextSchema;

public class JavaFullTextStepGenerator {

  private static final Logger log = LogManager.getLogger();

  private final CodeGenerator codeGen;
  private final String className;
  private final DataSchemaTable table;
  private final SchemaVersion inputSchema;
  private final FullTextSchema fullTextSchema;
  private final String schemaVersionId;
  private final String inputClass;

  public JavaFullTextStepGenerator(final CodeGenerator codeGen, final SchemaVersion inputSchema,
      final DataSchemaTable table) {
    this.codeGen = codeGen;
    this.fullTextSchema = codeGen.getFullTextSchema();
    this.table = table;
    this.inputSchema = inputSchema;
    this.schemaVersionId = codeGen.getSchemaVersionId();
    this.className = getClassName(table.getTableName());
    this.inputClass = JavaBindingGenerator.javaClass(table.getTableName(), inputSchema.getPrefix());
  }

  public static String getClassName(final String tableName) {
    return JavaBindingGenerator.javaClass(tableName + "FullTextStep", "");
  }

  public void generate(final PrintStream out) throws VerificationException {
    log.info("Generating full text step " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersionId);

    out.println("package " + codeGen.getJavaFullTextStepPackage() + ";");
    out.println();

    outputImportStatements(out);
    out.println();
    out.println("public class " + className + " implements ProcessingStep {");
    out.println();
    outputProcessMethod(out);
    out.println("}");
  }

  // Generate the import statements required for the class. Only produce imports
  // for classes that are actually used (to avoid compiler warnings).
  private void outputImportStatements(final PrintStream out) throws VerificationException {
    out.println("import " + IOException.class.getName() + ";");
    out.println("import " + CloseableMap.class.getName() + ";");
    out.println("import " + DataTable.class.getName() + ";");
    if (!getFullTextColumns().isEmpty()) {
      out.println("import " + FullTextTable.class.getName() + ";");
    }
    out.println("import " + ProcessingStep.class.getName() + ";");
    out.println("import " + TableWriter.class.getName() + ";");
    out.println("import " + inputSchema.getJavaBindingPackage() + "." + inputClass + ";");
  }

  private void outputProcessMethod(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  public DataTable process(final DataTable record, ");
    out.println("      CloseableMap<String, TableWriter<DataTable>> extraOutputs) throws IOException {");
    out.println("    final " + inputClass + " in = (" + inputClass + ")record;");
    for (final String column : getFullTextColumns()) {
      final String key = fullTextSchema.get(table.getTableName()).getKey();
      final String keyGetter = JavaBindingGenerator.javaGetter(key);
      final String valueGetter = JavaBindingGenerator.javaGetter(column);
      out.println("    extraOutputs.getMap().get(\"" + column + "\").add(new FullTextTable(in."
          + keyGetter + "(), in." + valueGetter + "()));");
    }
    out.println("    return in;");
    out.println("  }");
  }

  private List<String> getFullTextColumns() throws VerificationException {
    if (!fullTextSchema.tableNames().contains(table.getTableName())) {
      return new ArrayList<String>();
    }
    return fullTextSchema.get(table.getTableName()).getColumns();
  }

}
