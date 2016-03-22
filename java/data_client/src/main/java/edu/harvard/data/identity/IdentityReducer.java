package edu.harvard.data.identity;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.FileTableReader;

public class IdentityReducer extends Reducer<LongWritable, HadoopIdentityKey, Text, NullWritable> {

  protected final TableFormat format;
  protected final Map<Long, IdentityMap> identities;

  public IdentityReducer() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
    this.identities = new HashMap<Long, IdentityMap>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (final FSDataInputStream inStream = fs.open(path);
          FileTableReader<IdentityMap> in = new FileTableReader<IdentityMap>(IdentityMap.class,
              format, inStream)) {
        for (final IdentityMap id : in) {
          identities.put(id.getCanvasDataId(), id);
        }
      }
    }
  }

  @Override
  public void reduce(final LongWritable canvasDataId, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final IdentityMap id = new IdentityMap();
    id.setCanvasDataId(canvasDataId.get());
    if (identities.containsKey(canvasDataId)) {
      id.setResearchId(identities.get(canvasDataId).getResearchId());
    } else {
      id.setResearchId(UUID.randomUUID().toString());
    }
    for (final HadoopIdentityKey value : values) {
      final Long canvasId = value.getIdentityMap().getCanvasId();
      final String huid = value.getIdentityMap().getHuid();
      final String xid = value.getIdentityMap().getXid();
      if (id.getCanvasId() == null && canvasId != null) {
        id.setCanvasId(canvasId);
      }
      if (id.getHuid() == null && huid != null) {
        id.setHuid(huid);
      }
      if (id.getXid() == null && xid != null) {
        id.setXid(xid);
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
