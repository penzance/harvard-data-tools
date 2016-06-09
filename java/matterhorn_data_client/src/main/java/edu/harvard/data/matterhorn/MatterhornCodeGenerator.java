package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
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

public class MatterhornCodeGenerator extends CodeGenerator {

  private static final int TRANFORMATION_PHASES = 2;

  private static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.matterhorn.bindings.phase0";
  private static final String PHASE_ONE_PACKAGE = "edu.harvard.data.matterhorn.bindings.phase1";
  private static final String PHASE_TWO_PACKAGE = "edu.harvard.data.matterhorn.bindings.phase2";
  private static final String PHASE_THREE_PACKAGE = "edu.harvard.data.matterhorn.bindings.phase3";
  private static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.matterhorn.identity";

  public static final String PHASE_ZERO_TABLES_JSON = "matterhorn/phase0_redshift_tables.json";
  public static final String PHASE_ONE_IDENTIFIERS_JSON = "matterhorn/phase1_identifiers.json";
  public static final String PHASE_TWO_ADDITIONS_JSON = "matterhorn/phase2_schema_additions.json";
  public static final String PHASE_THREE_ADDITIONS_JSON = "matterhorn/phase3_schema_additions.json";

  private final String schemaVersion;
  private final File gitDir;

  private final MatterhornDataConfig config;

  public MatterhornCodeGenerator(final String schemaVersion, final File gitDir, final File codeDir,
      final MatterhornDataConfig config) throws FileNotFoundException {
    super(codeDir, AwsUtils.key(config.getS3WorkingLocation()));
    this.config = config;
    if (!gitDir.exists() && gitDir.isDirectory()) {
      throw new FileNotFoundException(gitDir.toString());
    }
    this.gitDir = gitDir;
    this.schemaVersion = schemaVersion;
  }

  public static void main(final String[] args) throws IOException, DataConfigurationException,
      UnexpectedApiResponseException, SQLException, VerificationException {
    if (args.length != 4) {
      System.err.println(
          "Usage: schema_version /path/to/config1|/path/to/config2 /path/to/harvard-data-tools /path/to/output/directory");
    }
    final String schemaVersion = args[0];
    final String configFiles = args[1];
    final File gitDir = new File(args[2]);
    final File dir = new File(args[3]);
    if (!(gitDir.exists() && gitDir.isDirectory())) {
      throw new FileNotFoundException(gitDir.toString());
    }
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configFiles, false);
    new MatterhornCodeGenerator(schemaVersion, gitDir, dir, config).generate();
  }

  @Override
  protected GenerationSpec createGenerationSpec() throws IOException, VerificationException {
    // Specify the four versions of the table bindings
    final GenerationSpec spec = new GenerationSpec(TRANFORMATION_PHASES);
    spec.setJavaProjectName("matterhorn_generated_code");
    spec.setJavaTableEnumName("MatterhornTable");
    spec.setPrefixes("Phase0", "Phase1", "Phase2", "Phase3");
    spec.setHdfsDirectories(config.getHdfsDir(0), config.getHdfsDir(1), config.getHdfsDir(2),
        config.getHdfsDir(3));
    spec.setJavaBindingPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);
    spec.setJavaHadoopPackage(IDENTITY_HADOOP_PACKAGE);
    spec.setHadoopIdentityManagerClass("MatterhornIdentityHadoopManager");
    spec.setMainIdentifier(IdentifierType.HUID);
    spec.setHiveScriptDir(new File(gitDir, "hive/matterhorn"));
    spec.setConfig(config);

    // Set the four schema versions in the spec.
    final DataSchema schema0 = readSchema(schemaVersion);
    final List<DataSchema> schemas = transformSchema(schema0);
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2), schemas.get(3));
    return spec;
  }

  private static DataSchema readSchema(final String version)
      throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final ClassLoader classLoader = MatterhornCodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader
        .getResourceAsStream("matterhorn_schema_" + version + ".json")) {
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

}
