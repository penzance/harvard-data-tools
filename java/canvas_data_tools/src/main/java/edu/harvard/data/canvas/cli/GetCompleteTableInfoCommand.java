package edu.harvard.data.canvas.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.Argument;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataArtifact;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class GetCompleteTableInfoCommand implements Command {
  private static final Logger log = LogManager.getLogger();

  private static final String FULL_DUMP_DIR = "full_dump";

  @Argument(index = 0, usage = "Output file.", metaVar = "/path/to/script.sh", required = true)
  public File output;

  @Argument(index = 1, usage = "S3 archive location.", metaVar = "archive_bucket/canvas/dumps", required = true)
  public String archiveKey;

  @Argument(index = 2, usage = "S3 incoming data location.", metaVar = "incoming_bucket/canvas/dumps", required = true)
  public String incomingKey;

  @Argument(index = 3, usage = "AWS region.", metaVar = "us-east-1", required = true)
  public String region;

  @Argument(index = 4, usage = "Latest dump sequence. The generated script will bring copy all files to this dump in order to generate a current snapshot.", metaVar = "116", required = true)
  public long latestSequence;

  @Override
  public ReturnStatus execute(final CanvasDataConfig config, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException,
      VerificationException {
    final ApiClient api = new ApiClient(config.canvasDataHost, config.canvasApiKey,
        config.canvasApiSecret);
    final AwsUtils aws = new AwsUtils();
    final DataDump dump = api.getDump(latestSequence);
    if (dump == null) {
      log.error("Can't find dump sequence " + latestSequence);
      return ReturnStatus.ARGUMENT_ERROR;
    }
    final Map<String, Long> sequence = new HashMap<String, Long>();
    for (final DataArtifact artifact : dump.getArtifactsByTable().values()) {
      final String tableName = artifact.getTableName();
      final TableInfo info = TableInfo.find(tableName);
      sequence.put(tableName, info.getLastCompleteDumpSequence());
    }

    final S3ObjectId archive = AwsUtils.key(archiveKey.substring(0, archiveKey.indexOf("/")),
        archiveKey.substring(archiveKey.indexOf("/") + 1));

    final S3ObjectId incoming = AwsUtils.key(incomingKey.substring(0, incomingKey.indexOf("/")),
        archiveKey.substring(incomingKey.indexOf("/") + 1));

    try (PrintStream out = new PrintStream(new FileOutputStream(output))) {
      for (final String table : sequence.keySet()) {
        final S3ObjectId outKey = AwsUtils.key(incoming, FULL_DUMP_DIR, table);
        for (long i = sequence.get(table); i <= dump.getSequence(); i++) {
          final S3ObjectId inKey = AwsUtils.key(archive, String.format("%05d", i), table);
          if (!aws.listKeys(inKey).isEmpty()) {
            out.println("aws s3 cp s3://" + inKey.getBucket() + "/" + inKey.getKey() + " s3://"
                + outKey.getBucket() + "/" + outKey.getKey() + " --region " + region
                + " --recursive");
          }
        }
      }
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Get a JSON list of tables, with the latest non-incremental dump";
  }

}
