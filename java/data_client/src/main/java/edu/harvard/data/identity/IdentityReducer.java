package edu.harvard.data.identity;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.HdfsTableReader;

public class IdentityReducer extends Reducer<LongWritable, HadoopIdentityKey, Text, NullWritable> {
  private static final Logger log = LogManager.getLogger();

  protected final Map<Long, IdentityMap> identities;
  private TableFormat format;

  public IdentityReducer() {
    this.identities = new HashMap<Long, IdentityMap>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      log.info("Reading existing identities from " + path);
      try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
          format, fs, path)) {
        for (final IdentityMap id : in) {
          identities.put(id.getCanvasDataID(), id);
        }
      }
    }
    log.info("Read " + identities.size() + " existing identities in identity reducer");
  }

  @Override
  public void reduce(final LongWritable canvasDataId, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final IdentityMap id = new IdentityMap();
    id.setCanvasDataID(canvasDataId.get());
    if (identities.containsKey(canvasDataId)) {
      id.setResearchId(identities.get(canvasDataId).getResearchId());
    } else {
      id.setResearchId(UUID.randomUUID().toString());
    }
    for (final HadoopIdentityKey value : values) {
      final Long canvasId = value.getIdentityMap().getCanvasID();
      final String huid = value.getIdentityMap().getHUID();
      final String xid = value.getIdentityMap().getXID();
      if (id.getCanvasID() == null && canvasId != null) {
        id.setCanvasID(canvasId);
      }
      if (id.getHUID() == null && huid != null) {
        id.setHUID(huid);
      }
      if (id.getXID() == null && xid != null) {
        id.setXID(xid);
      }
    }
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(id.getFieldsAsList(format));
    }
    final Text csvText = new Text(writer.toString().trim());
    context.write(csvText, NullWritable.get());
  }
}
