package edu.harvard.data.canvas;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.JavaBindingGenerator;
import edu.harvard.data.generator.SchemaPhase;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.identity.LongIdentityScrubber;
import edu.harvard.data.schema.DataSchemaTable;

public class CanvasIdentityScrubberGenerator {

  private static final Logger log = LogManager.getLogger();

  private final DataSchemaTable table;
  private final Map<String, List<IdentifierType>> identities;
  private final String className;
  private final String schemaVersion;
  private final String hadoopPackage;
  private final String phase0ModelPackage;
  private final String phase0ModelClass;
  private final String phase1ModelPackage;
  private final String phase1ModelClass;

  public CanvasIdentityScrubberGenerator(final DataSchemaTable table,
      final Map<String, List<IdentifierType>> identities, final String className,
      final String schemaVersion, final String hadoopPackage, final SchemaPhase phase0,
      final SchemaPhase phase1) {
    this.table = table;
    this.identities = identities;
    this.className = className;
    this.schemaVersion = schemaVersion;
    this.hadoopPackage = hadoopPackage;
    this.phase0ModelPackage = phase0.getJavaBindingPackage();
    this.phase0ModelClass = JavaBindingGenerator.javaClass(table.getTableName(),
        phase0.getPrefix());
    this.phase1ModelPackage = phase1.getJavaBindingPackage();
    this.phase1ModelClass = JavaBindingGenerator.javaClass(table.getTableName(),
        phase1.getPrefix());
  }

  public void generate(final PrintStream out) throws VerificationException {
    log.info("Generating Hadoop Scrubber " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);

    out.println("package " + hadoopPackage + ";");
    out.println();
    outputImportStatements(out);
    out.println();
    out.println("public class " + className + " extends LongIdentityScrubber {");
    out.println();
    outputGetHadoopKey(out);
    out.println();
    outputPopulateRecord(out);
    out.println("}");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + CSVRecord.class.getCanonicalName() + ";");
    out.println("import " + IdentityMap.class.getCanonicalName() + ";");
    out.println("import " + DataTable.class.getCanonicalName() + ";");
    out.println("import " + LongIdentityScrubber.class.getCanonicalName() + ";");
    out.println("import " + phase0ModelPackage + "." + phase0ModelClass + ";");
    out.println("import " + phase1ModelPackage + "." + phase1ModelClass + ";");
  }

  private void outputPopulateRecord(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  protected DataTable populateRecord(final CSVRecord csvRecord) {");
    out.println("    final " + phase0ModelClass + " phase0 = new " + phase0ModelClass
        + "(format, csvRecord);");
    out.println(
        "    final " + phase1ModelClass + " phase1 = new " + phase1ModelClass + "(phase0);");
    for (final String idColumn : getCanvasDataIdColumns()) {
      final String getter = JavaBindingGenerator.javaGetter(idColumn);
      final String setter = JavaBindingGenerator
          .javaSetter(idColumn + IdentitySchemaTransformer.RESARCH_UUID_SUFFIX);
      out.println("    if (phase0." + getter + "() != null) {");
      out.println(
          "      phase1." + setter + "(identities.get(phase0." + getter + "()).getResearchId());");
      out.println("    }");
    }
    out.println("    return phase1;");
    out.println("  }");
  }

  private void outputGetHadoopKey(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  protected Long getHadoopKey(final IdentityMap id) {");
    out.println("    return id.getCanvasDataID();");
    out.println("  }");
  }

  private List<String> getCanvasDataIdColumns() throws VerificationException {
    final List<String> ids = new ArrayList<String>();
    for (final String columnName : identities.keySet()) {
      if (identities.get(columnName).contains(IdentifierType.CanvasDataID)) {
        ids.add(columnName);
      }
    }
    if (ids.size() == 0) {
      throw new VerificationException(
          "Table " + table.getTableName() + " does not have a CanvasDataID field");
    }
    return ids;
  }

}
