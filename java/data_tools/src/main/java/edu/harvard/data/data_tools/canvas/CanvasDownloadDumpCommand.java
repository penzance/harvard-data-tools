package edu.harvard.data.data_tools.canvas;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.Argument;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataClient;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.CanvasApiClient;
import edu.harvard.data.client.canvas.CanvasDataDump;
import edu.harvard.data.client.canvas.CanvasDataSchema;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.data_tools.Command;
import edu.harvard.data.data_tools.DumpInfo;
import edu.harvard.data.data_tools.ReturnStatus;
import edu.harvard.data.data_tools.VerificationException;
import edu.harvard.data.data_tools.canvas.phase0.CanvasDumpManager;

public class CanvasDownloadDumpCommand implements Command {

  private static final Logger log = LogManager.getLogger();

  @Argument(index = 0, usage = "Metadata output file.", metaVar = "/path/to/file.json", required = true)
  private File output;

  @Override
  public ReturnStatus execute(final DataConfiguration config, final ExecutorService exec) throws IOException,
  DataConfigurationException, UnexpectedApiResponseException, VerificationException {
    final AwsUtils aws = new AwsUtils();
    final CanvasDumpManager manager = new CanvasDumpManager(config, aws);
    final CanvasApiClient api = DataClient.getCanvasApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    final List<CanvasDataDump> orderedDumps = sortDumps(api.getDumps());
    for (final CanvasDataDump dump : orderedDumps) {
      if (manager.needToSaveDump(dump)) {
        final CanvasDataDump fullDump = api.getDump(dump.getDumpId());
        final CanvasDataSchema schema = (CanvasDataSchema) api
            .getSchema(fullDump.getSchemaVersion());
        log.info("Saving " + fullDump.getSequence());
        final long start = System.currentTimeMillis();
        try {
          final Map<String, String> metadata = new HashMap<String, String>();
          final DumpInfo info = new DumpInfo(fullDump.getDumpId(), fullDump.getSequence(),
              fullDump.getSchemaVersion());
          info.save();
          manager.saveDump(api, fullDump, info);
          final S3ObjectId dumpLocation = manager.finalizeDump(fullDump, schema);
          metadata.put("AWS_BUCKET", dumpLocation.getBucket());
          metadata.put("AWS_KEY", dumpLocation.getKey());
          metadata.put("DUMP_ID", fullDump.getDumpId());
          metadata.put("DUMP_SEQUENCE", "" + fullDump.getSequence());
          writeMetadata(metadata);
          info.setBucket(dumpLocation.getBucket());
          info.setKey(dumpLocation.getKey());
          info.setDownloaded(true);
          info.save();
          manager.updateTableInfoTable(fullDump);
        } finally {
          manager.deleteTemporaryDump(fullDump);
        }
        final long time = (System.currentTimeMillis() - start) / 1000;
        log.info(
            "Downloaded and archived dump " + fullDump.getSequence() + " in " + time + " seconds");
        break;
      }
    }
    return ReturnStatus.OK;
  }

  private List<CanvasDataDump> sortDumps(final List<CanvasDataDump> dumps) {
    final List<CanvasDataDump> sortedDumps = new ArrayList<CanvasDataDump>(dumps);
    Collections.sort(sortedDumps, new Comparator<CanvasDataDump>() {
      @Override
      public int compare(final CanvasDataDump d1, final CanvasDataDump d2) {
        if (d1.getSequence() == d2.getSequence()) {
          return 0;
        }
        return d1.getSequence() < d2.getSequence() ? -1 : 1;
      }
    });
    return sortedDumps;
  }

  private void writeMetadata(final Map<String, String> metadataMap) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.writeValue(output, metadataMap);
    try (PrintStream out = new PrintStream(new FileOutputStream(output))) {
      out.println(mapper.writeValueAsString(metadataMap).replaceAll("  ", "    "));
    }
  }

  @Override
  public String getDescription() {
    return "Download and archive the oldest Canvas Data dump that is not already archived.";
  }

}
