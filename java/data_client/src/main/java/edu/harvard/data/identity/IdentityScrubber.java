package edu.harvard.data.identity;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.FileTableReader;

public class IdentityScrubber extends Mapper<Object, Text, Text, NullWritable> {

  protected final TableFormat format;
  protected final Map<Long, IdentityMap> identities;

  public IdentityScrubber() {
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

  protected void writeRecord(final DataTable record, final Context context)
      throws IOException, InterruptedException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    final Text csvText = new Text(writer.toString().trim());
    context.write(csvText, NullWritable.get());
  }

}
