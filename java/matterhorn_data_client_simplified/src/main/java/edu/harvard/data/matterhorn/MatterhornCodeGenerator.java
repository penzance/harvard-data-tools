package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

public class MatterhornCodeGenerator extends CodeGenerator {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
    if (args.length != 5) {
      System.err.println(
          "Usage: schema_version /path/to/config1|/path/to/config2 /path/to/harvard-data-tools /path/to/output/directory, run_id");
    }
    final String schemaVersion = args[0];
    final String configFiles = args[1];
    final File dir = new File(args[2]);
    final String runId = args[3];
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configFiles, false);
    new MatterhornCodeGenerator(config, dir, runId, schemaVersion).generate();
  }

  public MatterhornCodeGenerator(final DataConfig config, final File codeDir, final String runId,
      final String schemaVersionId) {
    super(config, codeDir, runId, schemaVersionId);
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.HUID;
  }

  @Override
  protected String getInputSchemaOverrideResource() {
    return null;
  }

  @Override
  protected String getIdentifierResource() {
    return "matterhorn/identifiers.json";
  }

  @Override
  protected String getFullTextResource() {
    return null;
  }

  @Override
  protected List<String> getCustomTransformationResources() {
    return new ArrayList<String>();
  }

  @Override
  protected String getJavaProjectName() {
    return "matterhorn_generated_code_simplified";
  }

  @Override
  protected String getJavaTableEnumName() {
    return "MatterhornTable";
  }

  @Override
  public String getGeneratedCodeManagerClassName() {
    return "MatterhornGeneratedCodeManager";
  }

  @Override
  protected String getJavaPackageBase() {
    return "edu.harvard.data.matterhorn";
  }

  @Override
  protected DataSchema getInputSchema() throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
    final ClassLoader classLoader = MatterhornCodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader
        .getResourceAsStream("matterhorn_schema_" + schemaVersionId + ".json")) {
      final ExtensionSchema schema = jsonMapper.readValue(in, ExtensionSchema.class);
      for (final String tableName : schema.getTables().keySet()) {
        ((ExtensionSchemaTable) schema.getTables().get(tableName)).setTableName(tableName);
      }
      return schema;
    }
  }

}
