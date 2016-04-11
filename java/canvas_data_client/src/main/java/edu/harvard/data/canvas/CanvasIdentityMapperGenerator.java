package edu.harvard.data.canvas;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.JavaBindingGenerator;
import edu.harvard.data.generator.SchemaPhase;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.identity.LongIdentityMapper;
import edu.harvard.data.schema.DataSchemaTable;

public class CanvasIdentityMapperGenerator {

  private static final Logger log = LogManager.getLogger();

  private final DataSchemaTable table;
  private final Map<String, List<IdentifierType>> identities;
  private final String className;
  private final String schemaVersion;
  private final String hadoopPackage;
  private final String modelPackage;
  private final String modelClass;

  public CanvasIdentityMapperGenerator(final DataSchemaTable table,
      final Map<String, List<IdentifierType>> identities, final String className,
      final String schemaVersion, final String hadoopPackage, final SchemaPhase phase) {
    this.table = table;
    this.identities = identities;
    this.className = className;
    this.schemaVersion = schemaVersion;
    this.hadoopPackage = hadoopPackage;
    this.modelPackage = phase.getJavaBindingPackage();
    this.modelClass = JavaBindingGenerator.javaClass(table.getTableName(), phase.getPrefix());
  }

  public void generate(final PrintStream out) throws VerificationException {
    log.info("Generating Hadoop mapper " + className);
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);

    out.println("package " + hadoopPackage + ";");
    out.println();
    outputImportStatements(out);
    out.println();
    out.println("public class " + className + " extends LongIdentityMapper {");
    out.println();
    out.println("  private " + modelClass + " phase0;");
    out.println();
    outputReadRecord(out);
    out.println();
    outputGetHadoopKeys(out);
    out.println();
    outputPopulateIdentityMap(out);
    out.println("}");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + Map.class.getCanonicalName() + ";");
    out.println("import " + HashMap.class.getCanonicalName() + ";");
    out.println("import " + CSVRecord.class.getCanonicalName() + ";");
    if (hasMultiplexedIdentities()) {
      out.println("import " + IdentifierType.class.getCanonicalName() + ";");
    }
    out.println("import " + IdentityMap.class.getCanonicalName() + ";");
    out.println("import " + LongIdentityMapper.class.getCanonicalName() + ";");
    out.println("import " + modelPackage + "." + modelClass + ";");
  }

  private boolean hasMultiplexedIdentities() {
    for (final List<IdentifierType> identifiers : identities.values()) {
      if (identifiers.size() > 1) {
        return true;
      }
    }
    return false;
  }

  private void outputReadRecord(final PrintStream out) {
    out.println("  @Override");
    out.println("  protected void readRecord(final CSVRecord csvRecord) {");
    out.println("    this.phase0 = new " + modelClass + "(format, csvRecord);");
    out.println("  }");
  }

  private void outputGetHadoopKeys(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  protected Map<String, Long> getHadoopKeys() {");
    out.println("    Map<String, Long> keys = new HashMap<String, Long>();");
    for (final String column : getCanvasDataIdColumns()) {
      final String getter = JavaBindingGenerator.javaGetter(column);
      out.println("    keys.put(\"" + column + "\", phase0." + getter + "());");
    }
    out.println("    return keys;");
    out.println("  }");
  }

  private void outputPopulateIdentityMap(final PrintStream out) throws VerificationException {
    out.println("  @Override");
    out.println("  protected boolean populateIdentityMap(final IdentityMap $id) {");
    if (getCanvasDataIdColumns().size() > 1) {
      out.println(
          "    throw new RuntimeException(\"Can't populate identity map with multiple Canvas Data IDs\");");
    } else {
      out.println("    boolean populated = false;");
      for (final String columnName : identities.keySet()) {
        if (identities.get(columnName).size() == 1) {
          outputSimpleIdPopulation(out, columnName);
        } else {
          outputComplexIdPopulation(out, columnName);
        }
      }
      out.println("    return populated;");
    }
    out.println("  }");
  }

  private void outputComplexIdPopulation(final PrintStream out, final String columnName)
      throws VerificationException {
    final String getter = JavaBindingGenerator.javaGetter(columnName);
    final String fieldName = JavaBindingGenerator.javaVariable(columnName);
    final String fieldType = JavaBindingGenerator.javaType(table.getColumn(columnName));
    out.println("    if (phase0." + getter + "() != null) {");
    out.println("      " + fieldType + " " + fieldName + " = phase0." + getter + "();");
    for (final IdentifierType identifierType : identities.get(columnName)) {
      if (identifierType == IdentifierType.Other) {
        // We only strip out "Other" fields; it doesn't make sense to specify a
        // column as ["Other", "HUID"], for example.
        throw new VerificationException(
            "Table: " + table.getTableName() + " column: " + columnName + ". Can't use "
                + IdentifierType.Other + " type in combination with any other identifier");
      }
      final String setter = JavaBindingGenerator.javaSetter(identifierType.toString());
      out.println("      if (IdentifierType." + identifierType + ".getPattern().matcher("
          + fieldName + ").matches()) {");
      out.println("        $id." + setter + "(phase0." + getter + "());");
      out.println("        populated = true;");
      out.println("      }");
    }
    out.println("    }");
  }

  private void outputSimpleIdPopulation(final PrintStream out, final String columnName) {
    final IdentifierType identifierType = identities.get(columnName).get(0);
    // We only strip out fields marked as "Other"; we don't need to save their
    // values.
    if (identifierType != IdentifierType.Other) {
      final String getter = JavaBindingGenerator.javaGetter(columnName);
      final String setter = JavaBindingGenerator.javaSetter(identifierType.toString());
      out.println("    if (phase0." + getter + "() != null) {");
      out.println("      $id." + setter + "(phase0." + getter + "());");
      out.println("      populated = true;");
      out.println("    }");
    }
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
