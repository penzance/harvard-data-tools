package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;

public abstract class StringIdentityMapper
extends Mapper<Object, Text, Text, HadoopIdentityKey> {
  private static final Logger log = LogManager.getLogger();

  protected TableFormat format;

  protected abstract void readRecord(CSVRecord csvRecord);

  // Get a map in case there are two identifier fields but one of them is null.
  // This way we register that we're dealing with the second case in the map
  // method.
  protected abstract Map<String, String> getHadoopKeys();

  protected abstract boolean populateIdentityMap(IdentityMap id);

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      readRecord(csvRecord);
      final Map<String, String> hadoopKeys = getHadoopKeys();
      log.info("Hadoop keys: " + hadoopKeys);
      if (hadoopKeys.size() == 1) {
        // If there's only one main ID, we can use this table to figure
        // out other identities for that individual.
        final String hadoopKey = hadoopKeys.entrySet().iterator().next().getValue();
        final IdentityMap id = new IdentityMap();
        final boolean populated = populateIdentityMap(id);
        if (populated) {
          log.info("Writing identity " + hadoopKey);
          context.write(new Text(hadoopKey), new HadoopIdentityKey(id));
        }
      } else {
        // If there are multiple main IDs in the table, it's ambiguous as
        // to which individual other identifier fields may refer. We just log
        // the identifier and leave it at that.
        for (final String hadoopKey : hadoopKeys.values()) {
          if (hadoopKey != null) {
            context.write(new Text(hadoopKey), new HadoopIdentityKey(new IdentityMap()));
          }
        }
      }
    }
  }
}
