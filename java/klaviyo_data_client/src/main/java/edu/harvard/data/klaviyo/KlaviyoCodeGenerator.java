package edu.harvard.data.klaviyo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

public class KlaviyoCodeGenerator extends CodeGenerator {

  private static final Logger log = LogManager.getLogger();

  private static final int TRANFORMATION_PHASES = 2;

  private static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.klaviyo.bindings.phase0";
  private static final String PHASE_ONE_PACKAGE = "edu.harvard.data.klaviyo.bindings.phase1";
  private static final String PHASE_TWO_PACKAGE = "edu.harvard.data.klaviyo.bindings.phase2";
  private static final String PHASE_THREE_PACKAGE = "edu.harvard.data.klaviyo.bindings.phase3";
  private static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.klaviyo.identity";

  public static final String PHASE_ZERO_TABLES_JSON = "klaviyo/phase0_redshift_tables.json";
  public static final String PHASE_ONE_IDENTIFIERS_JSON = "klaviyo/phase1_identifiers.json";
  public static final String PHASE_TWO_ADDITIONS_JSON = "klaviyo/phase2_schema_additions.json";
  public static final String PHASE_THREE_ADDITIONS_JSON = "klaviyo/phase3_schema_additions.json";
  public static final String FULL_TEXT_TABLES = "klaviyo/full_text_tables.json";

  private final String schemaVersion;
  private final File gitDir;

  private final KlaviyoDataConfig config;
  private final BootstrapParameters bootstrapParams;

  public KlaviyoCodeGenerator(final String schemaVersion, final File gitDir, final File codeDir,
      final KlaviyoDataConfig config, final String runId, BootstrapParameters bootstrapParams) throws FileNotFoundException {
    super(config, codeDir, runId);
    this.config = config;
    if (!gitDir.exists() && gitDir.isDirectory()) {
      throw new FileNotFoundException(gitDir.toString());
    }
    this.gitDir = gitDir;
    this.schemaVersion = schemaVersion;
    this.bootstrapParams = bootstrapParams;
  }

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
	log.info(args);
    if (args.length != 5) {
      System.err.println(
          "Usage: schema_version /path/to/config1|/path/to/config2 /path/to/harvard-data-tools /path/to/output/directory, run_id");
    }
    final String schemaVersion = args[0];
    final String configFiles = args[1];
    final File gitDir = new File(args[2]);
    final File dir = new File(args[3]);
    final String runId = args[4];
    final String bootstrapParamsString = args[5];
    if (!(gitDir.exists() && gitDir.isDirectory())) {
      throw new FileNotFoundException(gitDir.toString());
    }
    final KlaviyoDataConfig config = KlaviyoDataConfig
        .parseInputFiles(KlaviyoDataConfig.class, configFiles, false, bootstrapParamsString );
    log.info(args[5]);
    final BootstrapParameters bootstrapParams = new ObjectMapper().readValue(bootstrapParamsString,
	        BootstrapParameters.class);
    log.info(bootstrapParams.getConfigPathString());
    log.info(bootstrapParams.getRapidConfigDict());
    new KlaviyoCodeGenerator( schemaVersion, gitDir, dir, config, runId, bootstrapParams ).generate();
  }

  @Override
  protected GenerationSpec createGenerationSpec() throws IOException, VerificationException {
    // Specify the four versions of the table bindings
    final GenerationSpec spec = new GenerationSpec(TRANFORMATION_PHASES, schemaVersion);
    spec.setJavaProjectName("klaviyo_generated_code");
    spec.setJavaTableEnumName("KlaviyoTable");
    spec.setPrefixes("Phase0", "Phase1", "Phase2", "Phase3");
    spec.setHdfsDirectories(config.getHdfsDir(0), config.getHdfsDir(1), config.getHdfsDir(2),
        config.getHdfsDir(3));
    spec.setJavaBindingPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);
    spec.setJavaHadoopPackage(IDENTITY_HADOOP_PACKAGE);
    spec.setHadoopIdentityManagerClass("KlaviyoIdentityHadoopManager");
    spec.setMainIdentifier(IdentifierType.HUID);
    spec.setHiveScriptDir(new File(gitDir, "hive/klaviyo"));
    spec.setConfig(config);
    
    // Set string for Bootstrap Rapid Code JSON requests
    spec.setBootstrapRapidConfig(bootstrapParams.getRapidConfigDict());

    // Set the four schema versions in the spec.
    final DataSchema schema0 = readSchema(schemaVersion);
    final List<DataSchema> schemas = transformSchema(schema0);
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2), schemas.get(3));
    return spec;
  }

  private static DataSchema readSchema(final String version)
      throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.MEDIASITES_DATE_FORMAT_STRING));
    jsonMapper.setSerializationInclusion(Include.NON_NULL);
    final ClassLoader classLoader = KlaviyoCodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader
        .getResourceAsStream("klaviyo_schema_" + version + ".json")) {
      final ExtensionSchema schema = jsonMapper.readValue(in, ExtensionSchema.class);
      for (final String tableName : schema.getTables().keySet()) {
        ((ExtensionSchemaTable) schema.getTables().get(tableName)).setTableName(tableName);
      }
      return schema;
    }
  }

  @Override
  protected String getIdentifierResource() {
    return PHASE_ONE_IDENTIFIERS_JSON;
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.HUID;
  }

  @Override
  protected String getPhaseTwoAdditionsResource() {
    return PHASE_TWO_ADDITIONS_JSON;
  }

  @Override
  protected String getPhaseThreeAdditionsResource() {
    return PHASE_THREE_ADDITIONS_JSON;
  }

  @Override
  protected String getExistingTableResource() {
    return PHASE_ZERO_TABLES_JSON;
  }

  @Override
  protected String getFullTextResource() {
    return FULL_TEXT_TABLES;
  }

  @Override
  protected String getPhaseZeroModificationResource() {
    return null;
  }

}
