package edu.harvard.data.canvas;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasCodeGenerator extends CodeGenerator {

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
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configFiles, false);
    new CanvasCodeGenerator(config, dir, runId, schemaVersion).generate();
  }

  public CanvasCodeGenerator(final DataConfig config, final File codeDir, final String runId,
      final String schemaVersionId) {
    super(config, codeDir, runId, schemaVersionId);
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.CanvasDataID;
  }

  @Override
  protected String getInputSchemaOverrideResource() {
    return "canvas/phase0_schema_overrides.json";
  }

  @Override
  protected String getIdentifierResource() {
    return "canvas/identifiers.json";
  }

  @Override
  protected String getFullTextResource() {
    return "canvas/full_text.json";
  }

  @Override
  protected List<String> getCustomTransformationResources() {
    return new ArrayList<String>();
  }

  @Override
  protected String getJavaProjectName() {
    return "canvas_generated_code_simplified";
  }

  @Override
  protected String getJavaTableEnumName() {
    return "CanvasTable";
  }

  @Override
  public String getGeneratedCodeManagerClassName() {
    return "CanvasGeneratedCodeManager";
  }

  @Override
  protected String getJavaPackageBase() {
    return "edu.harvard.data.canvas";
  }

  @Override
  protected DataSchema getInputSchema() throws JsonParseException, JsonMappingException,
  IOException, DataConfigurationException, UnexpectedApiResponseException {
    // Get the specified schema version (or fail if that version doesn't exist).
    final String host = ((CanvasDataConfig) config).getCanvasDataHost();
    final String key = ((CanvasDataConfig) config).getCanvasApiKey();
    final String secret = ((CanvasDataConfig) config).getCanvasApiSecret();
    final ApiClient api = new ApiClient(host, key, secret);
    return api.getSchema(schemaVersionId);
  }

}
