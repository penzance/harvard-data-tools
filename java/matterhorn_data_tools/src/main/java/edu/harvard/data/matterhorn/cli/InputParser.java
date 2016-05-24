package edu.harvard.data.matterhorn.cli;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.JsonFileReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.EventJsonDocumentParser;
import edu.harvard.data.matterhorn.MatterhornDataConfiguration;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Event;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Video;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final MatterhornDataConfiguration config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  private final String outputBucket;
  private File originalFile;
  private File eventFile;
  private File videoFile;
  private S3ObjectId eventOutputObj;
  private S3ObjectId videoOutputObj;
  private final TableFormat inFormat;
  private final TableFormat outFormat;

  public InputParser(final MatterhornDataConfiguration config, final AwsUtils aws,
      final S3ObjectId inputObj, final String outputBucket) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.outputBucket = outputBucket;
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Matterhorn);
    this.outFormat = formatLibrary.getFormat(Format.CanvasDataFlatFiles);
  }

  public void parseFile() throws IOException {
    try {
      getFileNames();
      aws.getFile(inputObj, originalFile);
      parse();
      verify();
      aws.putFile(eventOutputObj, eventFile);
      aws.putFile(videoOutputObj, videoFile);
    } finally {
      cleanup();
    }
  }

  private void getFileNames() {
    final String key = inputObj.getKey();
    final String filename = key.substring(key.lastIndexOf("/") + 1);
    final String date = filename.substring(filename.indexOf(".") + 1, filename.indexOf(".jsonl"));
    originalFile = new File(config.getScratchDir(), filename);
    final String eventFileName = "event-" + date + ".gz";
    final String videoFileName = "video-" + date + ".gz";
    eventFile = new File(config.getScratchDir(), eventFileName);
    videoFile = new File(config.getScratchDir(), videoFileName);
    eventOutputObj = AwsUtils.key(outputBucket, "event", eventFileName);
    videoOutputObj = AwsUtils.key(outputBucket, "video", videoFileName);
    log.info("Parsing " + filename + " to " + eventFile + ", " + videoFile);
    log.info("Event key: " + eventOutputObj);
    log.info("Video key: " + videoOutputObj);
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    try (
        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
            new EventJsonDocumentParser(inFormat, true));
        TableWriter<Phase0Event> events = new TableWriter<Phase0Event>(Phase0Event.class,
            outFormat, eventFile);
        TableWriter<Phase0Video> videos = new TableWriter<Phase0Video>(Phase0Video.class,
            outFormat, videoFile);) {
      for (final Map<String, ? extends DataTable> tables : in) {
        events.add((Phase0Event) tables.get("event"));
        if (tables.containsKey("video")) {
          videos.add((Phase0Video) tables.get("video"));
        }
      }
    }
  }

  private void verify() throws IOException {
    try (FileTableReader<Phase0Event> in = new FileTableReader<Phase0Event>(Phase0Event.class,
        outFormat, eventFile)) {
      log.info("Verifying file " + eventFile);
      for (@SuppressWarnings("unused")
      final Phase0Event i : in) {
      }
    }
    try (FileTableReader<Phase0Video> in = new FileTableReader<Phase0Video>(Phase0Video.class,
        outFormat, videoFile)) {
      log.info("Verifying file " + videoFile);
      for (@SuppressWarnings("unused")
      final Phase0Video i : in) {
      }
    }
  }

  private void cleanup() {
    if (originalFile != null && originalFile.exists()) {
      originalFile.delete();
    }
    if (eventFile != null && eventFile.exists()) {
      eventFile.delete();
    }
    if (videoFile != null && videoFile.exists()) {
      videoFile.delete();
    }
  }

}
