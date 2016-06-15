package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.pipeline.Phase0Bootstrap;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0Bootstrap extends Phase0Bootstrap {

  private DataDump dump;

  private static final Logger log = LogManager.getLogger();

  @Override
  public String handleRequest(final String configPathString, final Context context) {
    try {
      super.init(configPathString, CanvasDataConfig.class);
      super.run();
    } catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
      return "Error: " + e.getMessage();
    }
    return "";
  }

  @Override
  protected boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final CanvasDataConfig canvasConfig = (CanvasDataConfig) config;
    DumpInfo.init(canvasConfig.getDumpInfoDynamoTable());
    final ApiClient api = new ApiClient(canvasConfig.getCanvasDataHost(),
        canvasConfig.getCanvasApiKey(), canvasConfig.getCanvasApiSecret());
    dump = null;
    for (final DataDump candidate : api.getDumps()) {
      if (needToSaveDump(candidate)) {
        dump = candidate;
        log.info("Saving dump " + dump);
        return true;
      }
    }
    return false;
  }

  @Override
  protected List<S3ObjectId> getInfrastructureConfigPaths() {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final S3ObjectId configPath = AwsUtils.key(config.getCodeBucket(), config.getGitTagOrBranch());
    paths.add(AwsUtils.key(configPath, "tiny_phase_0.properties"));
    paths.add(AwsUtils.key(configPath, "tiny_emr.properties"));
    return paths;
  }

  @Override
  protected Map<String, String> getCustomEc2Environment() {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("DATA_SET_ID", dump.getDumpId());
    env.put("DATA_SCHEMA_VERSION", "1.10.3"); // XXX: Remove
    return env;
  }

  private boolean needToSaveDump(final DataDump candidate) throws IOException {
    if (!candidate.getSchemaVersion().equals("1.10.3")) {
      return false;
    }
    final DumpInfo info = DumpInfo.find(candidate.getDumpId());
    if (candidate.getSequence() < 189) {
      log.warn("Dump downloader set to ignore dumps with sequence < 189");
      return false;
    }
    if (info == null) {
      log.info("Dump needs to be saved; no dump info record for " + candidate.getDumpId());
      return true;
    }
    if (info.getDownloaded() == null || !info.getDownloaded()) {
      log.info("Dump needs to be saved; previous download did not complete.");
      return true;
    }
    if (info.getVerified() == null || !info.getVerified()) {
      log.info("Dump needs to be saved; previous download did not properly verify.");
      return true;
    }
    final Date downloadStart = info.getDownloadStart();
    // Re-download any dump that was updated less than an hour before it was
    // downloaded before.
    final Date conservativeStart = new Date(downloadStart.getTime() - (60 * 60 * 1000));
    if (conservativeStart.before(candidate.getUpdatedAt())) {
      log.info(
          "Dump needs to be saved; previously downloaded less than an hour after it was last updated.");
      return true;
    }
    log.info("Dump does not need to be saved; already exists at " + info.getBucket() + "/"
        + info.getKey() + ".");
    return false;
  }

}
