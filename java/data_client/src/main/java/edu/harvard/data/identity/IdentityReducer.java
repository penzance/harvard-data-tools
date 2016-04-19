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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.HdfsTableReader;

/**
 * Helper class that implements the actual logic for the reduce phase during the
 * identity generation job. Since Hadoop requires that Hadoop-specific wrapper
 * types for the input key and value types be specified as part of the reducer
 * class declaration, we declare a separate reducer class for each possible type
 * for a data set's main identifier. See {@code LongIdentityReducer} for an
 * example.
 *
 * @param <T>
 *          the Java language class of the reducer's input key type. For
 *          example, a reducer that consumes
 *          {@link org.apache.hadoop.io.LongWritable} keys should declare an
 *          {@code IdentityReducer} parameterized with {@link Long}.
 */
public class IdentityReducer<T> {

  private static final Logger log = LogManager.getLogger();

  final Map<T, IdentityMap> identities;
  private TableFormat format;
  private final IdentifierType mainIdentifier;

  /**
   * Create a new {@code IdentityReducer}.
   *
   * @param mainIdentifier
   *          the {@link IdentifierType} that represents the primary identifier
   *          used in the data set. This is generally the primary key in a
   *          <code>users</code> table, or some external user identifier. The
   *          type of the identifier (determined by
   *          {@link IdentifierType#getType}) must be the same as the class
   *          parameter {@code T}.
   */
  public IdentityReducer(final IdentifierType mainIdentifier) {
    this.mainIdentifier = mainIdentifier;
    this.identities = new HashMap<T, IdentityMap>();
  }

  /**
   * Perform initial setup tasks before running the reducer. This method should
   * be called by the {@code setup} method of the actual reduce Hadoop task.
   *
   * This method populates two fields in the class. First, it retrieves the
   * format configuration setting from the Hadoop context and converts it into a
   * {@link Format} instance in order to correctly parse the incoming data.
   *
   * It then fetches the incoming identity map files from the Hadoop distributed
   * cache and builds up a map from the main identifier type to identity map
   * values that can be used during the reduce phase to ensure that consistent
   * research UUIDs are assigned to existing users.
   *
   * @param context
   *          the Hadoop context for the reducer.
   * @throws IOException
   *           if an error occurs while reading and parsing the identity map
   *           files in the Hadoop distributed cache.
   */
  public void setup(final Reducer<?, HadoopIdentityKey, Text, NullWritable>.Context context)
      throws IOException {
    this.format = HadoopJob.getFormat(context);
    readIdentityMap(context);
  }

  @SuppressWarnings("unchecked")
  private void readIdentityMap(
      final Reducer<?, HadoopIdentityKey, Text, NullWritable>.Context context) throws IOException {
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    log.info("Reading existing identities from " + context.getCacheFiles().length + " files");
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
          format, fs, path)) {
        for (final IdentityMap id : in) {
          identities.put((T) id.get(mainIdentifier), id);
        }
      }
    }
    log.info("Read " + identities.size() + " existing identities in identity reducer");
  }

  public void reduce(final T mainIdValue, final Iterable<HadoopIdentityKey> values,
      final Reducer<?, HadoopIdentityKey, Text, NullWritable>.Context context)
          throws IOException, InterruptedException {
    final IdentityMap id;
    if (identities.containsKey(mainIdValue)) {
      id = identities.get(mainIdValue);
    } else {
      id = new IdentityMap();
      id.set(mainIdentifier, mainIdValue);
      id.set(IdentifierType.ResearchUUID, UUID.randomUUID().toString());
    }
    for (final HadoopIdentityKey value : values) {
      for (final IdentifierType type : IdentifierType.values()) {
        if (type != mainIdentifier) {
          final Object obj = value.getIdentityMap().get(type);
          if (obj != null && id.get(type) == null) {
            id.set(type, obj);
          }
        }
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
