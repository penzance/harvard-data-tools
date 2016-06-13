package edu.harvard.data.canvas;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.pipeline.DataPipelineGenerator;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPipelineBootstrap {

  private static final Logger log = LogManager.getLogger();
  private static File gitDir;

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException {
    CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, args[0],
        false);
    gitDir = new File(args[1]);
    final String runId = args[2];
    final String host = config.getCanvasDataHost();
    final String key = config.getCanvasApiKey();
    final String secret = config.getCanvasApiSecret();
    final ApiClient api = new ApiClient(host, key, secret);
    final DataDump dump = api.getDump(229);
    final String emrConfig = chooseEmrConfigFile(dump);
    config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, args[0] + "|" + emrConfig,
        true);
    setupRun(dump, config, api, runId);
  }

  private static String chooseEmrConfigFile(final DataDump dump) {
    return "s3://hdt-code/api_pipeline/tiny_emr.properties";
  }

  private static void setupRun(final DataDump dump, final CanvasDataConfig config,
      final ApiClient api, final String runId) throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException {
    log.info("Saving dump " + dump);
    final S3ObjectId dumpLocation = AwsUtils.key(config.getS3IncomingLocation(),
        String.format("%05d", dump.getSequence()));
    final CanvasCodeGenerator generator = new CanvasCodeGenerator(dump.getSchemaVersion(), gitDir,
        null, config, null);
    final GenerationSpec spec = generator.createGenerationSpec();
    final DataPipelineGenerator pipeline = new DataPipelineGenerator(
        "Canvas_Dump_" + dump.getSequence(), spec, config, dumpLocation, new CanvasCodeManager(), runId);
    pipeline.generate();
  }

}