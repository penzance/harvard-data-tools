package edu.harvard.data.generator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.CloseableMap;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataTable;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.identity.IdentitySchema;

public class JavaIdentityStepGenerator {

  private static final Logger log = LogManager.getLogger();

  private final CodeGenerator codeGen;
  private final String className;
  private final DataSchemaTable table;
  private final SchemaVersion inputSchema;
  private final SchemaVersion outputSchema;
  private final IdentitySchema identitySchema;
  private final String schemaVersionId;
  private final String inputClass;
  private final String outputClass;
  private final IdentifierType mainIdentifier;

  public JavaIdentityStepGenerator(final CodeGenerator codeGen, final SchemaVersion inputSchema,
      final SchemaVersion outputSchema, final DataSchemaTable table) {
    this.codeGen = codeGen;
    this.table = table;
    this.identitySchema = codeGen.getIdentitySchema();
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this.schemaVersionId = codeGen.getSchemaVersionId();
    this.className = getClassName(table.getTableName());
    this.inputClass = JavaBindingGenerator.javaClass(table.getTableName(), inputSchema.getPrefix());
    this.outputClass = JavaBindingGenerator.javaClass(table.getTableName(),
        outputSchema.getPrefix());
    this.mainIdentifier = codeGen.getMainIdentifier();
  }

  public static String getClassName(final String tableName) {
    return JavaBindingGenerator.javaClass(tableName + "IdentityStep", "");
  }

  public void generate(final PrintStream out) throws VerificationException {
    log.info("Generating identity step " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersionId);

    out.println("package " + codeGen.getJavaIdentityStepPackage() + ";");
    out.println();

    outputImportStatements(out);
    out.println();
    out.println("public class " + className + " implements ProcessingStep {");
    out.println();
    if (!getMainIdColumns().isEmpty()) {
      outputFields(out);
      out.println();
    }
    outputConstructor(out);
    out.println();
    outputProcessMethod(out);
    if (!getMainIdColumns().isEmpty()) {
      out.println();
      outputGetResearchIdMethod(out);
    }
    out.println();
    outputCallMethod(out);
    out.println("}");
  }

  // Generate the import statements required for the class. Only produce imports
  // for classes that are actually used (to avoid compiler warnings).
  private void outputImportStatements(final PrintStream out) throws VerificationException {
    out.println("import " + CloseableMap.class.getName() + ";");
    out.println("import " + DataConfig.class.getName() + ";");
    out.println("import " + DataTable.class.getName() + ";");
    out.println("import " + ProcessingStep.class.getName() + ";");
    if (!getMainIdColumns().isEmpty()) {
      out.println("import " + IdentifierType.class.getName() + ";");
      out.println("import " + IdentityMap.class.getName() + ";");
    }
    out.println("import " + IdentityService.class.getName() + ";");
    out.println("import " + inputSchema.getJavaBindingPackage() + "." + inputClass + ";");
    out.println("import " + outputSchema.getJavaBindingPackage() + "." + outputClass + ";");
    out.println("import " + TableWriter.class.getName() + ";");
  }

  private void outputFields(final PrintStream out) {
    out.println("  private final IdentityService idService;");
    out.println("  private final DataConfig config;");
  }

  private void outputConstructor(final PrintStream out) throws VerificationException {
    out.println(
        "  public " + className + "(final IdentityService idService, final DataConfig config) {");
    if (!getMainIdColumns().isEmpty()) {
      out.println("    this.idService = idService;");
      out.println("    this.config = config;");
    }
    out.println("  }");
  }

  private void outputProcessMethod(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  public DataTable process(final DataTable record, ");
    out.println("      CloseableMap<String, TableWriter<DataTable>> extraOutputs) {");
    out.println("    final " + inputClass + " in = (" + inputClass + ")record;");
    out.println("    final " + outputClass + " out = new " + outputClass + "(in);");
    for (final String idColumn : getMainIdColumns()) {
      final String getter = JavaBindingGenerator.javaGetter(idColumn);
      final String setter = JavaBindingGenerator.javaSetter(idColumn + "ResearchUuid");
      out.println("    out." + setter + "(getResearchId(in." + getter + "()));");
    }
    out.println("    return out;");
    out.println("  }");
  }

  private void outputGetResearchIdMethod(final PrintStream out) throws VerificationException {
    out.println("  private String getResearchId(final Object mainId) {");
    out.println("    final IdentityMap id = new IdentityMap();");
    out.println("        id.set(IdentifierType." + mainIdentifier + ", mainId);");
    out.println("    return idService.getResearchUuid(id, config.getMainIdentifier());");
    out.println("  }");
  }

  private void outputCallMethod(final PrintStream out) {
    out.println("  @Override");
    out.println("  public Void call() throws Exception {");
    out.println("    return null;");
    out.println("  }");
  }

  private List<String> getMainIdColumns() throws VerificationException {
    final Map<String, List<IdentifierType>> identities = identitySchema.get(table.getTableName());
    if (identities == null || identities.isEmpty()) {
      return new ArrayList<String>();
    }
    final List<String> ids = new ArrayList<String>();
    for (final String columnName : identities.keySet()) {
      if (identities.get(columnName).contains(mainIdentifier)) {
        ids.add(columnName);
      }
    }
    return ids;
  }

}
