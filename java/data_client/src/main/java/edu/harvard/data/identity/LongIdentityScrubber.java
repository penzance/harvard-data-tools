package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.HdfsTableReader;

public abstract class LongIdentityScrubber extends Mapper<Object, Text, Text, NullWritable> {
  private static final Logger log = LogManager.getLogger();

  protected TableFormat format;
  protected final Map<Long, IdentityMap> identities;

  public LongIdentityScrubber() {
    this.identities = new HashMap<Long, IdentityMap>();
  }

  protected abstract DataTable populateRecord(CSVRecord csvRecord);
  protected abstract Long getHadoopKey(IdentityMap id);

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
          format, fs, path)) {
        for (final IdentityMap id : in) {
          identities.put(getHadoopKey(id), id);
        }
      }
    }
    log.info("Completed setup for " + this);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final DataTable record = populateRecord(csvRecord);
      context.write(HadoopJob.recordToText(record, format), NullWritable.get());
    }
  }
}
