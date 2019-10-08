package edu.harvard.data.canvas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.BootstrapParameters;
import edu.harvard.data.pipeline.Phase0Bootstrap;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0Bootstrap extends Phase0Bootstrap
implements RequestStreamHandler, RequestHandler<BootstrapParameters, String> {

  private DataDump dump;
  private BootstrapParameters params;
  private String schemaVersion;
  private String dumpId;

  private static final Logger log = LogManager.getLogger();

  // Main method for testing
  public static void main(final String[] args)
      throws JsonParseException, JsonMappingException, IOException {
    System.out.println(args[0]);
    final BootstrapParameters params = new ObjectMapper().readValue(args[0],
        BootstrapParameters.class);
    System.out.println(new CanvasPhase0Bootstrap().handleRequest(params, null));
  }
  
  @Override
  public String handleRequest(final BootstrapParameters params, final Context context) {
    try {
      super.init(params.getConfigPathString(), CanvasDataConfig.class, !params.getDownloadOnly());
      this.params = params;
      super.run(context);
    } catch (final Throwable e) {
      e.printStackTrace();
      return "Error: " + e.getMessage();
    }
    return "";
  }

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context) {
    try {
      final String requestjson = IOUtils.toString(inputStream, "UTF-8");
	  log.info("Params: " + requestjson);
      this.params = new ObjectMapper().readValue(requestjson, BootstrapParameters.class);
      params.setCreatePipeline();
      log.info(params.getConfigPathString());
      log.info(params.getRapidConfigDict());
      log.info(params.getCreatePipeline());
      super.init(params.getConfigPathString(), CanvasDataConfig.class, params.getCreatePipeline(), requestjson );
      super.run(context);
	} catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
	      log.info("Error: " + e.getMessage());
    }
  }

  @Override
  protected void setup()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final CanvasDataConfig canvasConfig = (CanvasDataConfig) config;
    final ApiClient api = new ApiClient(canvasConfig.getCanvasDataHost(),
        canvasConfig.getCanvasApiKey(), canvasConfig.getCanvasApiSecret());
    final List<String> args = new ArrayList<String>();
    log.info(params.toString());
    if (params.getDumpSequence() != null) {
      dump = api.getDump(params.getDumpSequence());
      args.add("DUMP:" + dump.getDumpId());
      schemaVersion = dump.getSchemaVersion();
    }
    if (params.getTable() != null) {
      args.add("TABLE:" + params.getTable());
      final DataDump latest = api.getLatestDump();
      schemaVersion = latest.getSchemaVersion();
    }
    if (params.getDumpSequence() == null && params.getTable() == null) {
      final List<DataDump> dumps = api.getDumps();
      Collections.reverse(dumps);
      for (final DataDump candidate : dumps) {
        if (needToSaveDump(candidate)) {
          dump = api.getDump(candidate.getDumpId());
          args.add("DUMP:" + dump.getDumpId());
          schemaVersion = dump.getSchemaVersion();
          break;
        }
      }
    }
    if (args.isEmpty()) {
      dumpId = null;
    } else {
      String id = args.get(0);
      for (int i = 1; i < args.size(); i++) {
        id += ":" + args.get(i);
      }
      dumpId = id;
    }
  }

  @Override
  protected boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    log.info("Saving dump " + dumpId);
    return dumpId != null;
  }

  @Override
  protected List<S3ObjectId> getInfrastructureConfigPaths()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final S3ObjectId configPath = AwsUtils.key(config.getCodeBucket(), "infrastructure");

    boolean megadump = false;
    if (dump != null && dump.getArtifactsByTable().containsKey("requests")) {
      megadump = !dump.getArtifactsByTable().get("requests").isPartial();
    }
    if (dumpId == null || dumpId.equals("TABLE:requests")) {
      // Don't need to download data, but will have to process the full requests
      // table
      paths.add(AwsUtils.key(configPath, "tiny_phase_0.properties"));
      paths.add(AwsUtils.key(configPath, "large_emr.properties"));
    } else if (megadump) {
      // Need to download a full requests table, then process it.
      paths.add(AwsUtils.key(configPath, "large_phase_0.properties"));
      paths.add(AwsUtils.key(configPath, "large_emr.properties"));
    } else {
      // Regular dump; we're OK with minimal hardware.
      paths.add(AwsUtils.key(configPath, "tiny_phase_0.properties"));
      paths.add(AwsUtils.key(configPath, "tiny_emr.properties"));
    }

    if (megadump) {
      final String subject = "*** WARNING: Dump " + dump.getSequence() + " is a megadump. ***";
      final String msg = "Redshift must be resized prior to update. Dump information at "
          + config.getHdtMonitorUrl() + "/data_dashboard/canvas/dump/" + dumpId;
      sendSnsNotification(subject, msg, config.getSuccessSnsArn());
    }

    return paths;
  }

  @Override
  protected Map<String, String> getCustomEc2Environment()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("DATA_SET_ID", dumpId);
    env.put("DATA_SCHEMA_VERSION", schemaVersion); // XXX: Remove
    return env;
  }

  private boolean needToSaveDump(final DataDump candidate)
      throws IOException, DataConfigurationException {
    final CanvasDataConfig canvasConfig = (CanvasDataConfig) config;
    DumpInfo.init(canvasConfig.getDumpInfoDynamoTable());
    final DumpInfo info = DumpInfo.find(candidate.getDumpId());
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
      info.resetDownloadAndVerify();
      return true;
    }
    log.info("Dump does not need to be saved; already exists at " + info.getBucket() + "/"
        + info.getKey() + ".");
    return false;
  }

}
