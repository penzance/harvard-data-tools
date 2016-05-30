package edu.harvard.data.canvas.phase_0;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DownloadDump {
  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException, ArgumentError {
    final String configPathString = args[0];
    final String dumpId = args[1];
    final File output = new File(args[2]);
    final AwsUtils aws = new AwsUtils();
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, false);
    final DumpManager manager = new DumpManager(config, aws);
    final ApiClient api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
    final DataDump dump = api.getDump(dumpId);
    final CanvasDataSchema schema = (CanvasDataSchema) api.getSchema(dump.getSchemaVersion());
    DumpInfo.init(config.getDumpInfoDynamoTable());
    TableInfo.init(config.getTableInfoDynamoTable());
    log.info("Saving " + dump.getSequence());
    final long start = System.currentTimeMillis();
    try {
      final Map<String, String> metadata = new HashMap<String, String>();
      final DumpInfo info = new DumpInfo(dump.getDumpId(), dump.getSequence(),
          dump.getSchemaVersion());
      info.save();
      manager.saveDump(api, dump, info);
      final S3ObjectId dumpLocation = manager.finalizeDump(dump, schema);
      metadata.put("AWS_BUCKET", dumpLocation.getBucket());
      metadata.put("AWS_KEY", dumpLocation.getKey());
      metadata.put("DUMP_ID", dump.getDumpId());
      metadata.put("DUMP_SEQUENCE", "" + dump.getSequence());
      writeMetadata(metadata, output);
      info.setBucket(dumpLocation.getBucket());
      info.setKey(dumpLocation.getKey());
      info.setDownloaded(true);
      info.save();
      manager.updateTableInfoTable(dump);
    } finally {
      manager.deleteTemporaryDump(dump);
    }
    final long time = (System.currentTimeMillis() - start) / 1000;
    log.info("Downloaded and archived dump " + dump.getSequence() + " in " + time + " seconds");
  }

  private static void writeMetadata(final Map<String, String> metadataMap, final File output)
      throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.writeValue(output, metadataMap);
    try (PrintStream out = new PrintStream(new FileOutputStream(output))) {
      out.println(mapper.writeValueAsString(metadataMap).replaceAll("  ", "    "));
    }
  }

}
