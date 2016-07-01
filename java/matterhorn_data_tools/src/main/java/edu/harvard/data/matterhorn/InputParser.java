package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Event;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0GeoIp;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0UserAgent;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Video;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final MatterhornDataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  private File originalFile;
  private File eventFile;
  private File videoFile;
  private File geoipFile;
  private File userAgentFile;
  private S3ObjectId eventOutputObj;
  private S3ObjectId videoOutputObj;
  private S3ObjectId geoipOutputObj;
  private S3ObjectId userAgentOutputObj;
  private final TableFormat inFormat;
  private final TableFormat outFormat;
  private final S3ObjectId videoOutputDir;
  private final S3ObjectId eventOutputDir;
  private final S3ObjectId geoipOutputDir;
  private final S3ObjectId userAgentOutputDir;

  public InputParser(final MatterhornDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.eventOutputDir = AwsUtils.key(outputLocation, "event");
    this.videoOutputDir = AwsUtils.key(outputLocation, "video");
    this.geoipOutputDir = AwsUtils.key(outputLocation, "geo_ip");
    this.userAgentOutputDir = AwsUtils.key(outputLocation, "user_agent");
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Matterhorn);
    this.outFormat = formatLibrary.getFormat(config.getPipelineFormat());
  }

  public InputTableIndex parseFile() throws IOException {
    final InputTableIndex dataIndex = new InputTableIndex();
    try {
      getFileNames();
      aws.getFile(inputObj, originalFile);
      parse();
      verify();
      aws.putFile(eventOutputObj, eventFile);
      aws.putFile(videoOutputObj, videoFile);
      aws.putFile(geoipOutputObj, geoipFile);
      aws.putFile(userAgentOutputObj, userAgentFile);
      dataIndex.addFile("event", eventOutputObj, eventFile.length());
      dataIndex.addFile("video", videoOutputDir, videoFile.length());
      dataIndex.addFile("geo_ip", geoipOutputDir, geoipFile.length());
      dataIndex.addFile("user_agent", userAgentOutputDir, userAgentFile.length());
      cleanup(); // XXX Move to finally block.
    } finally {
      //      cleanup();
    }
    return dataIndex;
  }

  private void getFileNames() {
    final String key = inputObj.getKey();
    final String filename = key.substring(key.lastIndexOf("/") + 1);
    final String date = filename.substring(filename.indexOf(".") + 1, filename.indexOf(".json"));
    originalFile = new File(config.getScratchDir(), filename);
    final String eventFileName = "event-" + date + ".gz";
    final String videoFileName = "video-" + date + ".gz";
    final String geoipFileName = "geoip-" + date + ".gz";
    final String userAgentFileName = "user_agent-" + date + ".gz";
    eventFile = new File(config.getScratchDir(), eventFileName);
    videoFile = new File(config.getScratchDir(), videoFileName);
    geoipFile = new File(config.getScratchDir(), geoipFileName);
    userAgentFile = new File(config.getScratchDir(), userAgentFileName);
    eventOutputObj = AwsUtils.key(eventOutputDir, eventFileName);
    videoOutputObj = AwsUtils.key(videoOutputDir, videoFileName);
    geoipOutputObj = AwsUtils.key(geoipOutputDir, geoipFileName);
    userAgentOutputObj = AwsUtils.key(userAgentOutputDir, userAgentFileName);
    log.info("Parsing " + filename + " to " + eventFile + ", " + videoFile + ", " + geoipFile + ", "
        + userAgentFile);
    log.info("Event key: " + eventOutputObj);
    log.info("Video key: " + videoOutputObj);
    log.info("GeoIp key: " + geoipOutputObj);
    log.info("User Agent key: " + userAgentOutputObj);
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    try (
        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
            new EventJsonDocumentParser(inFormat, true));
        TableWriter<Phase0Event> events = new TableWriter<Phase0Event>(Phase0Event.class, outFormat,
            eventFile);
        TableWriter<Phase0Video> videos = new TableWriter<Phase0Video>(Phase0Video.class, outFormat,
            videoFile);
        TableWriter<Phase0GeoIp> geoips = new TableWriter<Phase0GeoIp>(Phase0GeoIp.class, outFormat,
            geoipFile);
        TableWriter<Phase0UserAgent> userAgents = new TableWriter<Phase0UserAgent>(
            Phase0UserAgent.class, outFormat, userAgentFile);) {
      for (final Map<String, List<? extends DataTable>> tables : in) {
        events.add((Phase0Event) tables.get("event").get(0));
        if (tables.containsKey("video") && !tables.get("video").isEmpty()) {
          videos.add((Phase0Video) tables.get("video").get(0));
        }
        if (tables.containsKey("geoip") && !tables.get("geoip").isEmpty()) {
          geoips.add((Phase0GeoIp) tables.get("geoip").get(0));
        }
        if (tables.containsKey("ua") && !tables.get("ua").isEmpty()) {
          userAgents.add((Phase0UserAgent) tables.get("ua").get(0));
        }
      }
    }
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    try (FileTableReader<Phase0Event> in = new FileTableReader<Phase0Event>(Phase0Event.class,
        outFormat, eventFile)) {
      log.info("Verifying file " + eventFile);
      for (final Phase0Event i : in) {
      }
    }
    try (FileTableReader<Phase0Video> in = new FileTableReader<Phase0Video>(Phase0Video.class,
        outFormat, videoFile)) {
      log.info("Verifying file " + videoFile);
      for (final Phase0Video i : in) {
      }
    }
    try (FileTableReader<Phase0GeoIp> in = new FileTableReader<Phase0GeoIp>(Phase0GeoIp.class,
        outFormat, geoipFile)) {
      log.info("Verifying file " + geoipFile);
      for (final Phase0GeoIp i : in) {
      }
    }
    try (FileTableReader<Phase0UserAgent> in = new FileTableReader<Phase0UserAgent>(
        Phase0UserAgent.class, outFormat, userAgentFile)) {
      log.info("Verifying file " + userAgentFile);
      for (final Phase0UserAgent i : in) {
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
    if (geoipFile != null && geoipFile.exists()) {
      geoipFile.delete();
    }
    if (userAgentFile != null && userAgentFile.exists()) {
      userAgentFile.delete();
    }
  }

}
