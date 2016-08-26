package edu.harvard.data.edx;

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
import edu.harvard.data.TableFormat.Compression;
import edu.harvard.data.edx.bindings.phase0.Phase0Event;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.JsonFileReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final EdxDataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  private File originalFile;
  private File eventFile;
  private S3ObjectId eventOutputObj;
  private final TableFormat inFormat;
  private final TableFormat outFormat;
  private final S3ObjectId eventOutputDir;

  public InputParser(final EdxDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.eventOutputDir = AwsUtils.key(outputLocation, "event");
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Matterhorn);
    this.outFormat = formatLibrary.getFormat(config.getPipelineFormat());
    this.outFormat.setCompression(Compression.Gzip);
  }

  public InputTableIndex parseFile() throws IOException {
    final InputTableIndex dataIndex = new InputTableIndex();
    try {
      getFileNames();
      aws.getFile(inputObj, originalFile);
      parse();
      verify();
      aws.putFile(eventOutputObj, eventFile);
      dataIndex.addFile("event", eventOutputObj, eventFile.length());
    } finally {
      cleanup();
    }
    return dataIndex;
  }

  private void getFileNames() {
    final String key = inputObj.getKey();
    final String filename = key.substring(key.lastIndexOf("/") + 1);
    final String date = filename.substring(filename.indexOf(".") + 1, filename.indexOf(".json"));
    originalFile = new File(config.getScratchDir(), filename);
    final String eventFileName = "event-" + date + ".gz";
    eventFile = new File(config.getScratchDir(), eventFileName);
    eventOutputObj = AwsUtils.key(eventOutputDir, eventFileName);
    log.info("Parsing " + filename + " to " + eventFile);
    log.info("Event key: " + eventOutputObj);
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    try (
        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
            new EventJsonDocumentParser(inFormat, true));
        TableWriter<Phase0Event> events = new TableWriter<Phase0Event>(Phase0Event.class, outFormat,
            eventFile);) {
      for (final Map<String, List<? extends DataTable>> tables : in) {
        events.add((Phase0Event) tables.get("event").get(0));
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
  }

  private void cleanup() {
    if (originalFile != null && originalFile.exists()) {
      originalFile.delete();
    }
    if (eventFile != null && eventFile.exists()) {
      eventFile.delete();
    }
  }

}
