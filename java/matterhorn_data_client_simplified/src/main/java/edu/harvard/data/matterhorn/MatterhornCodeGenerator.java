package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

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

public class MatterhornCodeGenerator extends CodeGenerator {

  private static final String INPUT_PACKAGE = "edu.harvard.data.matterhorn.bindings.input";
  private static final String DEIDENTIFIED_PACKAGE = "edu.harvard.data.matterhorn.bindings.deidentified";
  private static final String OUTPUT_PACKAGE = "edu.harvard.data.matterhorn.bindings.output";
  private static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.matterhorn.identity";

  public static final String IDENTIFIERS_JSON = "matterhorn/identifiers.json";
  public static final String SCHEMA_ADDITIONS_JSON = "matterhorn/schema_additions.json";

  private final String schemaVersion;

  private final MatterhornDataConfig config;

  public MatterhornCodeGenerator(final String schemaVersion, final File codeDir,
      final MatterhornDataConfig config, final String runId) throws FileNotFoundException {
    super(config, codeDir, runId);
    this.config = config;
    this.schemaVersion = schemaVersion;
  }

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
    if (args.length != 4) {
      System.err.println(
          "Usage: schema_version /path/to/config1|/path/to/config2 /path/to/output/directory, run_id");
    }
    final String schemaVersion = args[0];
    final String configFiles = args[1];
    final File dir = new File(args[2]);
    final String runId = args[3];
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configFiles, false);
    new MatterhornCodeGenerator(schemaVersion, dir, config, runId).generate();
  }

  @Override
  protected GenerationSpec createGenerationSpec() throws IOException, VerificationException {
    final GenerationSpec spec = new GenerationSpec(schemaVersion);
    spec.setJavaProjectName("matterhorn_generated_code_simplified");
    spec.setJavaTableEnumName("MatterhornTable");
    spec.setPrefixes("Input", "Deidentified", "Output");
    spec.setJavaBindingPackages(INPUT_PACKAGE, DEIDENTIFIED_PACKAGE, OUTPUT_PACKAGE);
    spec.setIdentityPackage(IDENTITY_HADOOP_PACKAGE);
    spec.setMainIdentifier(IdentifierType.HUID);
    spec.setConfig(config);

    final DataSchema inputSchema = readSchema(schemaVersion);
    final List<DataSchema> schemas = transformSchema(inputSchema);
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2));
    return spec;
  }

  private static DataSchema readSchema(final String version)
      throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
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
    return IDENTIFIERS_JSON;
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.HUID;
  }

  @Override
  protected String getSchemaAdditionsResource() {
    return SCHEMA_ADDITIONS_JSON;
  }

  @Override
  protected String getFullTextResource() {
    return null;
  }

  @Override
  protected String getPhaseZeroModificationResource() {
    return null;
  }

}
