package edu.harvard.data.edx;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.TableFormat.Compression;
import edu.harvard.data.edx.bindings.phase0.Phase0Event;
import edu.harvard.data.io.JsonFileReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final EdxDataConfig config;
  private final File inputFile;
  private final File outputFile;
  private final TableFormat inFormat;
  private final TableFormat outFormat;

  public InputParser(final EdxDataConfig config, final File inputFile, final File outputFile) {
    this.config = config;
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.DecompressedEdx);
    this.outFormat = formatLibrary.getFormat(config.getPipelineFormat());
    this.outFormat.setCompression(Compression.Gzip);
    this.inputFile = inputFile;
    this.outputFile = outputFile;
  }

  public InputTableIndex parseFile() throws IOException {
    final InputTableIndex dataIndex = new InputTableIndex();
    log.info("Parsing file " + inputFile);
    try (
        final JsonFileReader in = new JsonFileReader(inFormat, inputFile,
            new EventJsonDocumentParser(inFormat, true));
        TableWriter<Phase0Event> events = new TableWriter<Phase0Event>(Phase0Event.class, outFormat,
            outputFile);) {
      for (final Map<String, List<? extends DataTable>> tables : in) {
        events.add((Phase0Event) tables.get("event").get(0));
      }
    }
    return dataIndex;
  }

}
