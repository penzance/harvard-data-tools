package edu.harvard.data.canvas;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.generator.CreateHiveTableGenerator;
import edu.harvard.data.generator.CreateRedshiftTableGenerator;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.generator.HiveQueryManifestGenerator;
import edu.harvard.data.generator.JavaBindingGenerator;
import edu.harvard.data.generator.MoveUnmodifiedTableGenerator;
import edu.harvard.data.generator.S3ToRedshiftLoaderGenerator;
import edu.harvard.data.generator.SchemaTransformer;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.extension.ExtensionSchema;

public class CanvasCodeGenerator {
  private static final Logger log = LogManager.getLogger();

  public static final String HDFS_HIVE_QUERY_DIR = "HIVE_QUERY_DIR";
  public static final String HDFS_PHASE_0_DIR = "hdfs:///phase_0";
  public static final String HDFS_PHASE_1_DIR = "hdfs:///phase_1";
  public static final String HDFS_PHASE_2_DIR = "hdfs:///phase_2";
  public static final String HDFS_PHASE_3_DIR = "hdfs:///phase_3";

  private static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.canvas.bindings.phase0";
  private static final String PHASE_ONE_PACKAGE = "edu.harvard.data.canvas.bindings.phase1";
  private static final String PHASE_TWO_PACKAGE = "edu.harvard.data.canvas.bindings.phase2";
  private static final String PHASE_THREE_PACKAGE = "edu.harvard.data.canvas.bindings.phase3";
  private static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.canvas.identity";

  public static final String PHASE_ONE_IDENTIFIERS_JSON = "phase1_identifiers.json";
  public static final String PHASE_TWO_ADDITIONS_JSON = "phase2_schema_additions.json";
  public static final String PHASE_THREE_ADDITIONS_JSON = "phase3_schema_additions.json";

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
    if (args.length != 3) {
      System.err
      .println("Usage: schema_version /path/to/harvard-data-tools /path/to/output/directory");
    }
    final String schemaVersion = args[0];
    final File gitDir = new File(args[1]);
    final File dir = new File(args[2]);
    dir.mkdirs();

    // Specify the four versions of the table bindings
    final GenerationSpec spec = new GenerationSpec(4);
    spec.setOutputBaseDirectory(dir);
    spec.setJavaTableEnumName("CanvasTable");
    spec.setPrefixes("Phase0", "Phase1", "Phase2", "Phase3");
    spec.setHdfsDirectories(HDFS_PHASE_0_DIR, HDFS_PHASE_1_DIR, HDFS_PHASE_2_DIR, HDFS_PHASE_3_DIR);
    spec.setJavaBindingPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);
    spec.setJavaHadoopPackage(IDENTITY_HADOOP_PACKAGE);

    // Get the specified schema version (or fail if that version doesn't exist).
    final DataConfiguration config = DataConfiguration.getConfiguration("secure.properties");
    final String host = config.getCanvasDataHost();
    final String key = config.getCanvasApiKey();
    final String secret = config.getCanvasApiSecret();
    final ApiClient api = new ApiClient(host, key, secret);

    // Set the four schema versions in the spec.
    final List<DataSchema> schemas = transformSchema(api.getSchema(schemaVersion));
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2), schemas.get(3));

    // Generate the bindings.
    log.info("Generating Java bindings in " + dir);
    new JavaBindingGenerator(spec, "canvas_data_schema_bindings").generate();

    log.info("Generating Java identity Hadoop jobs in " + dir);
    new CanvasIdentityJobGenerator(spec, readIdentities(PHASE_ONE_IDENTIFIERS_JSON))
    .generate();

    log.info("Generating Hive table definitions in " + dir);
    new CreateHiveTableGenerator(dir, spec).generate();

    log.info("Generating Hive query manifests in " + dir);
    new HiveQueryManifestGenerator(gitDir, dir, spec).generate();

    log.info("Generating Redshift table definitions in " + dir);
    new CreateRedshiftTableGenerator(dir, spec).generate();

    log.info("Generating Redshift copy from S3 script in " + dir);
    new S3ToRedshiftLoaderGenerator(dir, spec).generate();

    log.info("Generating move unmodified files script in " + dir);
    new MoveUnmodifiedTableGenerator(dir, spec).generate();

  }

  public static List<DataSchema> transformSchema(final DataSchema base)
      throws VerificationException, IOException {
    // Transform the schema to remove identifers
    final IdentitySchemaTransformer idTrans = new IdentitySchemaTransformer();
    final DataSchema schema1 = idTrans.transform(base, readIdentities(PHASE_ONE_IDENTIFIERS_JSON));

    // Transform the schema using the additions specified in json resources.
    final ExtensionSchema phase2 = readExtensionSchema(PHASE_TWO_ADDITIONS_JSON);
    final ExtensionSchema phase3 = readExtensionSchema(PHASE_THREE_ADDITIONS_JSON);

    final SchemaTransformer transformer = new SchemaTransformer();
    final DataSchema schema2 = transformer.transform(schema1, phase2);
    final DataSchema schema3 = transformer.transform(schema2, phase3);

    final List<DataSchema> schemas = new ArrayList<DataSchema>();
    schemas.add(base);
    schemas.add(schema1);
    schemas.add(schema2);
    schemas.add(schema3);
    return schemas;
  }

  private static Map<String, Map<String, List<IdentifierType>>> readIdentities(
      final String jsonResource) throws IOException {
    log.info("Reading identifiers from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = CanvasCodeGenerator.class.getClassLoader();
    final TypeReference<Map<String, Map<String, List<IdentifierType>>>> identiferTypeRef = new TypeReference<Map<String, Map<String, List<IdentifierType>>>>() {
    };
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      return jsonMapper.readValue(in, identiferTypeRef);
    }
  }

  public static ExtensionSchema readExtensionSchema(final String jsonResource) throws IOException {
    log.info("Extending schema from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = CanvasCodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      return jsonMapper.readValue(in, ExtensionSchema.class);
    }
  }

}
