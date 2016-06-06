package edu.harvard.data.canvas;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.pipeline.DataPipelineGenerator;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasBootstrap {

  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException {
    CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, args[0], false);
    DumpInfo.init(config.dumpInfoDynamoTable);
    final String host = config.canvasDataHost;
    final String key = config.canvasApiKey;
    final String secret = config.canvasApiSecret;
    final ApiClient api = new ApiClient(host, key, secret);
    final DataDump dump = findNextDump(api);
    final String emrConfig = chooseEmrConfigFile(dump);
    config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, args[0] + "|" + emrConfig, true);
    if (dump == null) {
      log.info("No new dumps to download");
      sendSnsMessage(dump, config, "No new dumps to download",
          "Latest dump: " + api.getLatestDump().toString());
    } else {
      setupRun(dump, config, api, args[0]);
    }
  }

  private static String chooseEmrConfigFile(final DataDump dump) {
    return "s3://hdt-code/api_pipeline/tiny_emr.properties";
  }

  private static DataDump findNextDump(final ApiClient api)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    for (final DataDump dump : api.getDumps()) {
      if (needToSaveDump(dump)) {
        return dump;
      }
    }
    return null;
  }

  private static void setupRun(final DataDump dump, final CanvasDataConfig config,
      final ApiClient api, final String configFiles) throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException {
    log.info("Saving dump " + dump);
    final CanvasCodeGenerator generator = new CanvasCodeGenerator(dump.getSchemaVersion(),
        configFiles, new File("/Users/mcgachey/work/harvard/harvard-data-tools"), null, config);
    final GenerationSpec spec = generator.createGenerationSpec();
    final DataPipelineGenerator pipeline = new DataPipelineGenerator(
        "Canvas_Dump_" + dump.getSequence(), spec, config);
    pipeline.generate();
  }

  private static void sendSnsMessage(final DataDump dump, final CanvasDataConfig config,
      final String subject, final String msg) {
    final AmazonSNSClient sns = new AmazonSNSClient();
    final String topicArn = config.successSnsArn;
    final PublishRequest publishRequest = new PublishRequest(topicArn, msg, subject);
    sns.publish(publishRequest);
  }

  private static boolean needToSaveDump(final DataDump dump) throws IOException {
    final DumpInfo info = DumpInfo.find(dump.getDumpId());
    if (dump.getSequence() < 189) {
      return false;
    }
    if (info == null) {
      log.info("Dump needs to be saved; no dump info record for " + dump.getDumpId());
      return true;
    }
    if (info.getDownloaded() == null || !info.getDownloaded()) {
      log.info("Dump needs to be saved; previous download did not complete.");
      return true;
    }
    final Date downloadStart = info.getDownloadStart();
    // Re-download any dump that was updated less than an hour before it was
    // downloaded before.
    final Date conservativeStart = new Date(downloadStart.getTime() - (60 * 60 * 1000));
    if (conservativeStart.before(dump.getUpdatedAt())) {
      log.info(
          "Dump needs to be saved; previously downloaded less than an hour after it was last updated.");
      return true;
    }
    log.info("Dump does not need to be saved; already exists at " + info.getBucket() + "/"
        + info.getKey() + ".");
    return false;
  }

}
