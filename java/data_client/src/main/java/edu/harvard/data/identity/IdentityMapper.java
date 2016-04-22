package edu.harvard.data.identity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.TableFormat;
import edu.harvard.data.generator.IdentityMapperGenerator;

/**
 * Helper class that implements the common logic for the various mapper classes
 * used during the first identity Hadoop job. For every table that has been
 * determined to hold identifying information, a mapper is created that gathers
 * all identifiers for each record, grouping them by the data set's main
 * identifier. Is is recommended, although not required, that these map jobs be
 * generated; see {@link IdentityMapperGenerator} for details of the
 * table-specific classes.
 *
 * Since Hadoop requires that mappers declare the types
 *
 * @param <T>
 */
public class IdentityMapper<T> {
  private static final Logger log = LogManager.getLogger();
  TableFormat format;

  public IdentityMapper(final TableFormat format) {
    this.format = format;
  }

  public Map<T, HadoopIdentityKey> map(final Text value,
      final Mapper<?, ?, ?, HadoopIdentityKey>.Context context,
      final GeneratedIdentityMapper<T> idMapper) throws IOException, InterruptedException {
    final Map<T, HadoopIdentityKey> results = new HashMap<T, HadoopIdentityKey>();
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      try {
        idMapper.readRecord(csvRecord);
      } catch (final Throwable t) {
        final InputSplit fileSplit = (InputSplit) context.getInputSplit();
        final String filename = fileSplit.toString();
        throw new RuntimeException("Error parsing " + value + " in file " + filename);
      }
      final Map<String, T> hadoopKeys = idMapper.getHadoopKeys();
      log.info("Hadoop keys: " + hadoopKeys);
      if (hadoopKeys.size() == 1) {
        // If there's only one main ID, we can use this table to figure
        // out other identities for that individual.
        final T hadoopKey = hadoopKeys.entrySet().iterator().next().getValue();
        final IdentityMap id = new IdentityMap();
        final boolean populated = idMapper.populateIdentityMap(id);
        if (populated) {
          results.put(hadoopKey, new HadoopIdentityKey(id));
        }
      } else {
        // If there are multiple main IDs in the table, it's ambiguous as
        // to which individual other identifier fields may refer. We just log
        // the identifier and leave it at that.
        for (final T hadoopKey : hadoopKeys.values()) {
          if (hadoopKey != null) {
            results.put(hadoopKey, new HadoopIdentityKey(new IdentityMap()));
          }
        }
      }
    }
    return results;
  }

}
