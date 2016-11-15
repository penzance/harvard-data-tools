package edu.harvard.data.generator;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataTable;
import edu.harvard.data.GeneratedCodeManager;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.TableFactory;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.schema.fulltext.FullTextTable;

public class JavaGeneratedCodeManagerGenerator {

  private static final Logger log = LogManager.getLogger();

  private final CodeGenerator codeGen;
  private final SchemaVersion inputSchema;
  private final String schemaVersionId;
  private final String className;

  public JavaGeneratedCodeManagerGenerator(final CodeGenerator codeGen,
      final SchemaVersion inputSchema) {
    this.codeGen = codeGen;
    this.inputSchema = inputSchema;
    this.className = getClassName(codeGen);
    this.schemaVersionId = codeGen.getSchemaVersionId();
  }

  public static String getClassName(final CodeGenerator codeGen) {
    return JavaBindingGenerator.javaClass(codeGen.getGeneratedCodeManagerClassName(), "");
  }

  public void generate(final PrintStream out) throws VerificationException {
    log.info("Generating full text step " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersionId);

    out.println("package " + codeGen.getJavaPackageBase() + ";");
    out.println();

    outputImportStatements(out);
    out.println();
    out.println("public class " + className + " implements GeneratedCodeManager {");
    out.println();
    outputFields(out);
    out.println();
    outputConstructor(out);
    out.println();
    outputGetIdentityStep(out);
    out.println();
    outputGetFullTextStep(out);
    out.println();
    outputGetAllSteps(out);
    out.println();
    outputGetFullTextWriters(out);
    out.println("}");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + File.class.getName() + ";");
    out.println("import " + HashMap.class.getName() + ";");
    out.println("import " + HashSet.class.getName() + ";");
    out.println("import " + IOException.class.getName() + ";");
    out.println("import " + Map.class.getName() + ";");
    out.println("import " + Set.class.getName() + ";");
    out.println("import " + DataConfig.class.getName() + ";");
    out.println("import " + DataTable.class.getName() + ";");
    out.println("import " + GeneratedCodeManager.class.getName() + ";");
    out.println("import " + IdentityService.class.getName() + ";");
    out.println("import " + ProcessingStep.class.getName() + ";");
    out.println("import " + TableFactory.class.getName() + ";");
    out.println("import " + TableFormat.class.getName() + ";");
    out.println("import " + TableWriter.class.getName() + ";");
    final String idPkg = codeGen.getJavaIdentityStepPackage();
    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      final String className = JavaIdentityStepGenerator.getClassName(tableName);
      out.println("import " + idPkg + "." + className + ";");
    }
    final String txtPkg = codeGen.getJavaFullTextStepPackage();
    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      final String className = JavaFullTextStepGenerator.getClassName(tableName);
      out.println("import " + txtPkg + "." + className + ";");
    }
  }

  private void outputFields(final PrintStream out) {
    out.println("  private final Map<String, ProcessingStep> identitySteps;");
    out.println("  private final Map<String, ProcessingStep> fullTextSteps;");
  }

  private void outputConstructor(final PrintStream out) {
    out.println("  public " + className + "() {");
    out.println("    this.identitySteps = new HashMap<String, ProcessingStep>();");
    out.println("    this.fullTextSteps = new HashMap<String, ProcessingStep>();");
    out.println("  }");
  }

  private void outputGetIdentityStep(final PrintStream out) {
    out.println("  @Override");
    out.println(
        "  public ProcessingStep getIdentityStep(final String tableName, final IdentityService idService,");
    out.println("    final DataConfig config) {");
    out.println("    if (!identitySteps.containsKey(tableName)) {");
    out.println("      switch (tableName) {");
    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      out.println("      case \"" + tableName + "\":");
      out.println("        identitySteps.put(\"" + tableName + "\", new "
          + JavaIdentityStepGenerator.getClassName(tableName) + "(idService, config));");
      out.println("        break;");
    }
    out.println("      default:");
    out.println("        throw new RuntimeException(\"Unknown table \" + tableName);");
    out.println("      }");
    out.println("    }");
    out.println("    return identitySteps.get(tableName);");
    out.println("  }");
  }

  private void outputGetFullTextStep(final PrintStream out) {
    out.println("  @Override");
    out.println("  public ProcessingStep getFullTextStep(final String tableName) {");
    out.println("    if (!fullTextSteps.containsKey(tableName)) {");
    out.println("      switch (tableName) {");
    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      out.println("      case \"" + tableName + "\":");
      out.println("        fullTextSteps.put(\"" + tableName + "\", new "
          + JavaFullTextStepGenerator.getClassName(tableName) + "());");
      out.println("        break;");
    }
    out.println("      default:");
    out.println("        throw new RuntimeException(\"Unknown table \" + tableName);");
    out.println("      }");
    out.println("    }");
    out.println("    return fullTextSteps.get(tableName);");
    out.println("  }");
  }

  private void outputGetAllSteps(final PrintStream out) {
    out.println("  @Override");
    out.println("  public Set<ProcessingStep> getAllSteps() {");
    out.println("    Set<ProcessingStep> steps = new HashSet<ProcessingStep>();");
    out.println("    steps.addAll(identitySteps.values());");
    out.println("    steps.addAll(fullTextSteps.values());");
    out.println("    return steps;");
    out.println("  }");
  }

  private void outputGetFullTextWriters(final PrintStream out) {
    out.println("  @SuppressWarnings(\"unchecked\")");
    out.println("  @Override");
    out.println(
        "  public Map<String, TableWriter<DataTable>> getFullTextWriters(final TableFactory tableFactory,");
    out.println(
        "      final String tableName, final TableFormat format, final String tmpFileBase) throws IOException {");
    out.println(
        "    final Map<String, TableWriter<DataTable>> writers = new HashMap<String, TableWriter<DataTable>>();");
    out.println("    switch (tableName) {");
    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      final FullTextTable table = codeGen.getFullTextSchema().get(tableName);
      out.println("    case \"" + tableName + "\":");
      if (table != null) {
        for (final String column : table.getColumns()) {
          out.println("      writers.put(\"" + column
              + "\", (TableWriter<DataTable>) tableFactory.getTableWriter(tableName, format, new File(tmpFileBase + \"-"
              + column + ".gz\")));");
        }
      }
      out.println("      break;");
    }
    out.println("    default:");
    out.println("      throw new RuntimeException(\"Unknown table \" + tableName);");
    out.println("    }");
    out.println("    return writers;");
    out.println("  }");
  }

}
