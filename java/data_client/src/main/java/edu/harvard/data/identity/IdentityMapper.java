package edu.harvard.data.identity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopUtilities;
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
 * <p>
 * Since Hadoop requires that mappers declare the key and value types to be
 * processed as part of the mapper definition, there are several identity
 * mappers defined (one for each main identifier type). These mappers defer to
 * this class for common operations. See {@link LongIdentityMapper} for an
 * example of an identity mapper job.
 * <p>
 * @param <T>
 *          the Java type of the main identifier over which this job operates.
 *          Thus, for a mapper job that returns a map from {@code LongWritable}
 *          to {@code Text}, this parameter would be {@code Long}.
 */
public class IdentityMapper<T> {
  TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public IdentityMapper() {
    this.hadoopUtils = new HadoopUtilities();
  }

  public void setup(final Mapper<?, ?, ?, ?>.Context context) {
    this.format = hadoopUtils.getFormat(context);
  }
  

  /**
   * Process a single record to produce a map from the data set's main
   * identifier (of type {@code T}) to a {@link HadoopIdentityKey} object that
   * contains all identifiers that relate to a given individual. This method
   * uses a {@link TableIdentityMapper} to determine the specific identity
   * values that can be extracted from a given record.
   * <p>
   * Depending on the table being analyzed, there are two cases to consider. In
   * the first case, there is exactly one column in the table that contains the
   * dataset's main identifier (we assume the case with zero identifiers does
   * not occur, since such a table would not be specified as a candidate for the
   * identity Hadoop job). When there is one main identifier column, we assume
   * that any other identifier in the table relates to the same individual. Thus
   * for a hypothetical <code>user</code> table, the main identifier may be the
   * primary key, but other interesting identifiers may exist, such as foreign
   * keys into external systems. In that case, the {@code HadoopIdentityKey}
   * returned for that user will contain all the identifiers in the record.
   * <p>
   * In the second case, there may be multiple main identifier columns in the
   * table. This would be the case in a join table that connects two entries in
   * the <code>user</code> table together. In this case, we can't make any
   * assumptions around the other values in the record, since they may be
   * related to any one of the main identifiers. When we encounter this case we
   * record the values of the main identifiers, but do not populate the
   * {@code HadoopIdentityKey} objects any further.
   * <p>
   * @param value
   *          the Hadoop {@code Text} object that wraps the record to be
   *          processed by this method. The body of the {@code Text} object must
   *          be formatted according to the {@link TableFormat} specified in
   *          {@link #format}.
   * @param idMapper
   *          a table-specific mapper object that implements the
   *          {@code TableIdentityMapper} class. This object is used to
   *          determine which fields in the record represent identifiers.
   *
   * @return a {@link Map} from {@code T} (the main identifier type) to
   *         {@code HadoopIdentityKey} that contains the identity information
   *         discovered by this method. This map may be empty, but will never be
   *         null.
   *
   * @throws IOException if an error occurs when parsing the {@code value} text string.
   */
  public Map<T, HadoopIdentityKey> map(final Text value, final TableIdentityMapper<T> idMapper)
      throws IOException {
    final Map<T, HadoopIdentityKey> results = new HashMap<T, HadoopIdentityKey>();
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      idMapper.readRecord(csvRecord);
      final Map<String, T> hadoopKeys = idMapper.getMainIdentifiers();
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
