package edu.harvard.data.generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.existing.ExistingSchema;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

public abstract class CodeGenerator {

  private static final Logger log = LogManager.getLogger();

  public static final int PIPELINE_PHASES = 4;

  public static final String HDFS_HIVE_QUERY_DIR = "HIVE_QUERY_DIR";
  public static final String HDFS_PHASE_0_DIR = "hdfs:///phase_0";
  public static final String HDFS_PHASE_1_DIR = "hdfs:///phase_1";
  public static final String HDFS_PHASE_2_DIR = "hdfs:///phase_2";
  public static final String HDFS_PHASE_3_DIR = "hdfs:///phase_3";

  protected final File codeDir;
  protected final File gitDir;

  public CodeGenerator(final File gitDir, final File codeDir) {
    this.gitDir = gitDir;
    this.codeDir = codeDir;
  }

  protected abstract GenerationSpec createGenerationSpec() throws IOException,
  DataConfigurationException, VerificationException, UnexpectedApiResponseException;

  protected abstract String getExistingTableResource();

  protected abstract IdentifierType getMainIdentifier();

  protected abstract String getIdentifierResource();

  protected abstract String getPhaseTwoAdditionsResource();

  protected abstract String getPhaseThreeAdditionsResource();

  protected void generate() throws IOException, DataConfigurationException, VerificationException,
  UnexpectedApiResponseException {
    codeDir.mkdirs();

    final GenerationSpec spec = createGenerationSpec();
    spec.setOutputBaseDirectory(codeDir);

    // Generate the bindings.
    log.info("Generating Java bindings in " + codeDir);
    new JavaBindingGenerator(spec).generate();

    log.info("Generating Java identity Hadoop jobs in " + codeDir);
    new IdentityJobGenerator(spec, readIdentities(getIdentifierResource())).generate();

    log.info("Generating Hive table definitions in " + codeDir);
    new CreateHiveTableGenerator(codeDir, spec).generate();

    log.info("Generating Hive query manifests in " + codeDir);
    new HiveQueryManifestGenerator(codeDir, spec).generate();

    log.info("Generating Redshift table definitions in " + codeDir);
    new CreateRedshiftTableGenerator(codeDir, spec).generate();

    log.info("Generating Redshift copy from S3 script in " + codeDir);
    new S3ToRedshiftLoaderGenerator(codeDir, spec).generate();

    log.info("Generating move unmodified files script in " + codeDir);
    new MoveUnmodifiedTableGenerator(codeDir, spec).generate();
  }

  public List<DataSchema> transformSchema(final DataSchema base)
      throws VerificationException, IOException {
    // We will transform the schema using the additions specified in json
    // resources.
    final ExtensionSchema phase2 = readExtensionSchema(getPhaseTwoAdditionsResource());
    final ExtensionSchema phase3 = readExtensionSchema(getPhaseThreeAdditionsResource());

    // Transform the schema to remove identifers
    final IdentitySchemaTransformer idTrans = new IdentitySchemaTransformer();
    final DataSchema schema1 = idTrans.transform(base, readIdentities(getIdentifierResource()),
        getMainIdentifier());

    final SchemaTransformer transformer = new SchemaTransformer();
    final DataSchema schema2 = transformer.transform(schema1, phase2);
    final DataSchema schema3 = transformer.transform(schema2, phase3);

    // Add in any tables that are to be read from Redshift. This has to happen
    // last so that we have the correct schema for all existing tables; a table
    // that will be read from Redshift is always based on a table that was
    // previously written there.
    final ExistingTableSchemaTransformer existingTrans = new ExistingTableSchemaTransformer();
    final ExistingSchema existing = readExistingSchemas(getExistingTableResource());
    final DataSchema schema0 = existingTrans.transform(base, existing, schema3);

    final List<DataSchema> schemas = new ArrayList<DataSchema>();
    schemas.add(schema0);
    schemas.add(schema1);
    schemas.add(schema2);
    schemas.add(schema3);
    return schemas;
  }

  public static ExtensionSchema readExtensionSchema(final String jsonResource)
      throws IOException, VerificationException {
    log.info("Extending schema from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    final ExtensionSchema schema;
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      schema = jsonMapper.readValue(in, ExtensionSchema.class);
    }
    for (final String tableName : schema.getTables().keySet()) {
      ((ExtensionSchemaTable) schema.getTables().get(tableName)).setTableName(tableName);
    }
    verify(schema);
    return schema;
  }

  private static void verify(final ExtensionSchema schema) throws VerificationException {
    for (final DataSchemaTable table : schema.getTables().values()) {
      verifyTable(table);
      final Set<String> columnNames = new HashSet<String>();
      for (final DataSchemaColumn column : table.getColumns()) {
        verifyColumn(column, table.getTableName());
        if (columnNames.contains(column.getName())) {
          error(table, "defines column " + column.getName() + " more than once.");
        }
        columnNames.add(column.getName());
      }
    }
  }

  private static void verifyTable(final DataSchemaTable table) throws VerificationException {
    if (table.getExpirationPhase() != null) {
      if (table.getExpirationPhase() < 0) {
        error(table, "has a negative expiration phase");
      }
      if (table.getExpirationPhase() >= PIPELINE_PHASES) {
        error(table, "has an expiration phase greater than " + (PIPELINE_PHASES - 1));
      }

    }
  }

  private static void verifyColumn(final DataSchemaColumn column, final String tableName)
      throws VerificationException {
    if (column.getName() == null) {
      throw new VerificationException("Column of table " + tableName + " has no name");
    }
    if (column.getType() == null) {
      error(column, tableName, "has no type");
    }
    if (column.getType().equals(DataSchemaType.VarChar)) {
      if (column.getLength() == null) {
        error(column, tableName, "is of type varchar but has no length");
      }
      if (column.getLength() == 0) {
        error(column, tableName, "is of type varchar but has zero length");
      }
    }
  }

  private static void error(final DataSchemaTable table, final String msg) throws VerificationException {
    throw new VerificationException(
        "Table " + table.getTableName() + " " + msg);
  }

  private static void error(final DataSchemaColumn column, final String tableName, final String msg)
      throws VerificationException {
    throw new VerificationException(
        "Column " + column.getName() + " of table " + tableName + " " + msg);
  }

  public static ExistingSchema readExistingSchemas(final String jsonResource) throws IOException {
    log.info("Reading existing table schemas from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      final ExistingSchema schema = jsonMapper.readValue(in, ExistingSchema.class);
      for (final String tableName : schema.getTables().keySet()) {
        schema.getTables().get(tableName).setTableName(tableName);
      }
      return schema;
    }
  }

  public static Map<String, Map<String, List<IdentifierType>>> readIdentities(
      final String jsonResource) throws IOException {
    log.info("Reading identifiers from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    final TypeReference<Map<String, Map<String, List<IdentifierType>>>> identiferTypeRef = new TypeReference<Map<String, Map<String, List<IdentifierType>>>>() {
    };
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      return jsonMapper.readValue(in, identiferTypeRef);
    }
  }
}
