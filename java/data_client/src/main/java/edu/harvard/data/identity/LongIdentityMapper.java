package edu.harvard.data.identity;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;

public abstract class LongIdentityMapper
extends Mapper<Object, Text, LongWritable, HadoopIdentityKey> {

  protected final TableFormat format;

  public LongIdentityMapper() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  protected abstract void readRecord(CSVRecord csvRecord);

  protected abstract Long getHadoopKey();

  protected abstract boolean populateIdentityMap(IdentityMap id);

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      readRecord(csvRecord);
      final Long hadoopKey = getHadoopKey();
      final IdentityMap id = new IdentityMap();
      final boolean populated = populateIdentityMap(id);
      if (populated) {
        context.write(new LongWritable(hadoopKey), new HadoopIdentityKey(id));
      }
    }
  }
}
