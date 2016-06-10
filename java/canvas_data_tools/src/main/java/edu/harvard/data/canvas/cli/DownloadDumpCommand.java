package edu.harvard.data.canvas.cli;

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

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.phase_0.DumpManager;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DownloadDumpCommand implements Command {

  private static final Logger log = LogManager.getLogger();

  @Argument(index = 0, usage = "Metadata output file.", metaVar = "/path/to/file.json", required = true)
  private File output;

  @Override
  public ReturnStatus execute(final CanvasDataConfig config, final ExecutorService exec)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException,
      VerificationException, ArgumentError {
    final AwsUtils aws = new AwsUtils();
    final DumpManager manager = new DumpManager(config, aws);
    final ApiClient api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
    final List<DataDump> orderedDumps = sortDumps(api.getDumps());
    for (final DataDump dump : orderedDumps) {
      if (manager.needToSaveDump(dump)) {
        final DataDump fullDump = api.getDump(dump.getDumpId());
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

  private List<DataDump> sortDumps(final List<DataDump> dumps) {
    final List<DataDump> sortedDumps = new ArrayList<DataDump>(dumps);
    Collections.sort(sortedDumps, new Comparator<DataDump>() {
      @Override
      public int compare(final DataDump d1, final DataDump d2) {
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
