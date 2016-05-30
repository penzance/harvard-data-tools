package edu.harvard.data.canvas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasCodeGenerator extends CodeGenerator {

  private static final int TRANFORMATION_PHASES = 2;

  private static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.canvas.bindings.phase0";
  private static final String PHASE_ONE_PACKAGE = "edu.harvard.data.canvas.bindings.phase1";
  private static final String PHASE_TWO_PACKAGE = "edu.harvard.data.canvas.bindings.phase2";
  private static final String PHASE_THREE_PACKAGE = "edu.harvard.data.canvas.bindings.phase3";
  private static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.canvas.identity";

  public static final String PHASE_ZERO_TABLES_JSON = "canvas/phase0_redshift_tables.json";
  public static final String PHASE_ONE_IDENTIFIERS_JSON = "canvas/phase1_identifiers.json";
  public static final String PHASE_TWO_ADDITIONS_JSON = "canvas/phase2_schema_additions.json";
  public static final String PHASE_THREE_ADDITIONS_JSON = "canvas/phase3_schema_additions.json";

  private final String schemaVersion;
  private final File gitDir;
  private final CanvasDataConfig config;

  public CanvasCodeGenerator(final String schemaVersion, final File gitDir, final File codeDir,
      final CanvasDataConfig config, final String runId) throws FileNotFoundException {
    super(config, codeDir, runId);
    this.gitDir = gitDir;
    this.schemaVersion = schemaVersion;
    this.config = config;
  }

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
    if (args.length != 5) {
      System.err.println(
          "Usage: schema_version /path/to/config1|/path/to/config2 /path/to/harvard-data-tools /path/to/output/directory, run_id");
    }
    final String schemaVersion = args[0];
    final String configFiles = args[1];
    final File gitDir = new File(args[2]);
    final File dir = new File(args[3]);
    final String runId = args[4];
    if (!(gitDir.exists() && gitDir.isDirectory())) {
      throw new FileNotFoundException(gitDir.toString());
    }
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configFiles, false);
    new CanvasCodeGenerator(schemaVersion, gitDir, dir, config, runId).generate();
  }

  @Override
  protected GenerationSpec createGenerationSpec() throws IOException, DataConfigurationException,
  VerificationException, UnexpectedApiResponseException {
    // Specify the four versions of the table bindings
    final GenerationSpec spec = new GenerationSpec(TRANFORMATION_PHASES, schemaVersion);
    spec.setJavaProjectName("canvas_generated_code");
    spec.setJavaTableEnumName("CanvasTable");
    spec.setPrefixes("Phase0", "Phase1", "Phase2", "Phase3");
    spec.setHdfsDirectories(config.getHdfsDir(0), config.getHdfsDir(1), config.getHdfsDir(2),
        config.getHdfsDir(3));
    spec.setJavaBindingPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);
    spec.setJavaHadoopPackage(IDENTITY_HADOOP_PACKAGE);
    spec.setHadoopIdentityManagerClass("CanvasIdentityHadoopManager");
    spec.setMainIdentifier(IdentifierType.CanvasDataID);
    spec.setHiveScriptDir(new File(gitDir, "hive/canvas"));
    spec.setConfig(config);

    // Get the specified schema version (or fail if that version doesn't exist).
    final String host = config.getCanvasDataHost();
    final String key = config.getCanvasApiKey();
    final String secret = config.getCanvasApiSecret();
    final ApiClient api = new ApiClient(host, key, secret);

    // Set the four schema versions in the spec.
    final List<DataSchema> schemas = transformSchema(api.getSchema(schemaVersion));
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2), schemas.get(3));
    return spec;
  }

  @Override
  protected String getIdentifierResource() {
    return PHASE_ONE_IDENTIFIERS_JSON;
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.CanvasDataID;
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
