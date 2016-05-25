package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.identity.IdentitySchema;

public class IdentityJobGenerator {

  private static final Logger log = LogManager.getLogger();
  private final DataSchema schema;
  private final IdentitySchema identities;
  private final GenerationSpec spec;
  private final File javaSrcBase;

  public IdentityJobGenerator(final GenerationSpec spec,
      final IdentitySchema identities) {
    this.identities = identities;
    this.schema = spec.getPhase(0).getSchema();
    this.spec = spec;
    final String srcPath = spec.getIdentityHadoopPackage().replaceAll("\\.", File.separator);
    this.javaSrcBase = new File(spec.getOutputBase(), "java/src/main/java/" + srcPath);
  }

  public void generate() throws IOException, VerificationException {
    javaSrcBase.mkdirs();

    final List<String> tableNames = new ArrayList<String>();
    final List<String> mapperNames = new ArrayList<String>();
    final List<String> scrubberNames = new ArrayList<String>();
    final String hadoopPackage = spec.getIdentityHadoopPackage();
    final String version = schema.getVersion();

    for (final String tableName : identities.tableNames()) {
      final String classBase = JavaBindingGenerator.javaClass(tableName, "");
      final Map<String, List<IdentifierType>> tableIds = identities.get(tableName);
      final DataSchemaTable table = schema.getTableByName(tableName);
      final SchemaPhase phase0 = spec.getPhase(0);
      final SchemaPhase phase1 = spec.getPhase(1);

      final String mapperClass = classBase + "IdentityMapper";
      final File mapperFile = new File(javaSrcBase, mapperClass + ".java");
      try (final PrintStream out = new PrintStream(new FileOutputStream(mapperFile))) {
        log.info("Generating " + mapperClass + " at " + mapperFile);
        new IdentityMapperGenerator(table, tableIds, mapperClass, version, hadoopPackage, phase0,
            spec.getMainIdentifier()).generate(out);
      }

      final String scrubberClass = classBase + "IdentityScrubber";
      final File scrubberFile = new File(javaSrcBase, scrubberClass + ".java");
      try (final PrintStream out = new PrintStream(new FileOutputStream(scrubberFile))) {
        log.info("Generating " + scrubberClass + " at " + scrubberFile);
        new IdentityScrubberGenerator(table, tableIds, scrubberClass, version, hadoopPackage,
            phase0, phase1, spec.getMainIdentifier()).generate(out);
      }

      tableNames.add(tableName);
      mapperNames.add(mapperClass);
      scrubberNames.add(scrubberClass);
    }

    final String managerClass = spec.getHadoopIdentityManagerClass();
    final File managerFile = new File(javaSrcBase, managerClass + ".java");
    try (final PrintStream out = new PrintStream(new FileOutputStream(managerFile))) {
      new IdentityManagerGenerator(hadoopPackage, managerClass, tableNames, mapperNames,
          scrubberNames).generate(out);
    }
  }

  static List<String> getMainIdColumns(final Map<String, List<IdentifierType>> identities,
      final DataSchemaTable table, final IdentifierType mainIdentifier) throws VerificationException {
    final List<String> ids = new ArrayList<String>();
    for (final String columnName : identities.keySet()) {
      if (identities.get(columnName).contains(mainIdentifier)) {
        ids.add(columnName);
      }
    }
    if (ids.size() == 0) {
      throw new VerificationException(
          "Table " + table.getTableName() + " does not have a field of type " + mainIdentifier);
    }
    return ids;
  }

}
