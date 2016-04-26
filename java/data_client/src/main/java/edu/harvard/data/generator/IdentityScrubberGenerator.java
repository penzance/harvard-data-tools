package edu.harvard.data.generator;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.identity.IdentityScrubber;
import edu.harvard.data.schema.DataSchemaTable;

public class IdentityScrubberGenerator {

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
  private final IdentifierType mainIdentifier;

  public IdentityScrubberGenerator(final DataSchemaTable table,
      final Map<String, List<IdentifierType>> identities, final String className,
      final String schemaVersion, final String hadoopPackage, final SchemaPhase phase0,
      final SchemaPhase phase1, final IdentifierType mainIdentifier) {
    this.table = table;
    this.identities = identities;
    this.className = className;
    this.schemaVersion = schemaVersion;
    this.hadoopPackage = hadoopPackage;
    this.mainIdentifier = mainIdentifier;
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

    final String idType = mainIdentifier.getType().getSimpleName();
    out.println("package " + hadoopPackage + ";");
    out.println();
    outputImportStatements(out, idType);
    out.println();
    out.println("public class " + className + " extends IdentityScrubber<" + idType + "> {");
    out.println();
    outputPopulateRecord(out);
    out.println("}");
  }

  private void outputImportStatements(final PrintStream out, final String idType) {
    out.println("import " + CSVRecord.class.getCanonicalName() + ";");
    out.println("import " + IdentifierType.class.getCanonicalName() + ";");
    out.println("import " + DataTable.class.getCanonicalName() + ";");
    out.println("import " + IdentityScrubber.class.getCanonicalName() + ";");
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
    for (final String idColumn : IdentityJobGenerator.getMainIdColumns(identities, table,
        mainIdentifier)) {
      final String getter = JavaBindingGenerator.javaGetter(idColumn);
      final String setter = JavaBindingGenerator
          .javaSetter(idColumn + IdentitySchemaTransformer.RESEARCH_UUID_SUFFIX);
      out.println("    if (phase0." + getter + "() != null) {");
      out.println("      phase1." + setter + "((String) identities.get(phase0." + getter
          + "()).get(IdentifierType.ResearchUUID));");
      out.println("    }");
    }
    out.println("    return phase1;");
    out.println("  }");
  }

}
