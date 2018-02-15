package edu.harvard.data.identity;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.TableReader;

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

  Map<T, IdentityMap> identities;
  TableFormat format;
  IdentifierType mainIdentifier;
  private final HadoopUtilities hadoopUtils;

  public IdentityReducer() {
    this.hadoopUtils = new HadoopUtilities();
  }
  

  /**
   * Perform initial setup tasks before running the reducer. This method should
   * be called by the {@code setup} method of the actual identity reducer Hadoop
   * task.
   * <p>
   * This method populates three fields in the class. First, it retrieves the
   * format configuration setting from the Hadoop context and converts it into a
   * {@link Format} instance in order to correctly parse the incoming data. It
   * does the same for the main identifier value: the {@link IdentifierType}
   * that represents the primary identifier used in the data set. This is
   * generally the primary key in a <code>users</code> table, or some external
   * user identifier. The type of the identifier (determined by
   * {@link IdentifierType#getType}) must be the same as the class parameter
   * {@code T}.
   * <p>
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
  @SuppressWarnings("unchecked")
  public void setup(final Reducer<?, ?, ?, ?>.Context context) throws IOException {
    this.format = hadoopUtils.getFormat(context);
    this.mainIdentifier = hadoopUtils.getMainIdentifier(context);
    this.identities = new HashMap<T, IdentityMap>();
    try (TableReader<IdentityMap> in = hadoopUtils.getHdfsTableReader(context, format,
        IdentityMap.class)) {
      for (final IdentityMap id : in) {
        identities.put((T) id.get(mainIdentifier), id);
      }
    }
  }

  /**
   * Merge the identities of a single user. This method operates over the set of
   * {@link IdentityMap} objects generated by the various identity mapper jobs.
   * For each main identifier it fist checks to see whether a user with that
   * identifier exists in the existing identity map. If not, it creates a new
   * identity for that individual. It then scans through all the identity
   * information gathered by the mappers to supplement the user's identity with
   * any newly-discovered identities.
   * <p>
   * This method should be called by the {@code reduce} method of the actual
   * identity reducer Hadoop task.
   * <p>
   * TODO: This method does not currently handle the case where different
   * mappers produce contradictory identities.
   * <p>
   *
   * @param mainIdValue
   *          the key used to identify this user in the data set.
   * @param values
   *          all identity values calculated by the various identity mapper
   *          jobs. The {@code IdentityMap} objects are wrapped in
   *          Hadoop-friendly {@link HadoopIdentityKey} objects.
   * @param outputs
   *          a MultipleOutputs instance that is configured to allow writing
   *          values to the identity map file, as well as any other identity
   *          outputs such as names or e-mail addresses. The names out of the
   *          output streams (other than identitymap) will match the result of
   *          {@link IdentifierType#getFieldName}.
   *
   * @throws IOException
   *           if an error occurs while outputting the identity object to the
   *           context.
   * @throws InterruptedException
   *           if interrupted while writing to the context.
   */
  public void reduce(final T mainIdValue, final Iterable<HadoopIdentityKey> values,
      final MultipleOutputs<Text, NullWritable> outputs) throws IOException, InterruptedException {
    final IdentityMap id;
    final Set<String> emails = new HashSet<String>();
    final Set<String> names = new HashSet<String>();
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
      final String email = (String) value.getIdentityMap().get(IdentifierType.EmailAddress);
      if (email != null && !email.isEmpty()) {
        emails.add(email);
      }
      final String name = (String) value.getIdentityMap().get(IdentifierType.Name);
      if (name != null && !name.isEmpty()) {
        names.add(name);
      }
    }
    outputResult("tempidentitymap", outputs, id.getFieldsAsList(format).toArray());    
    for (final String email : emails) {
      if (email != null && !email.isEmpty()) {	
    	  outputResult(IdentifierType.EmailAddress.getFieldName(), outputs,
    	     id.get(IdentifierType.ResearchUUID), email);
      }
    }
    for (final String name : names) {
      if (name != null && !name.isEmpty()) {
          outputResult(IdentifierType.Name.getFieldName(), outputs, id.get(IdentifierType.ResearchUUID),
             name);
      }
    }
  }

  private void outputResult(final String outputName,
      final MultipleOutputs<Text, NullWritable> outputs, final Object... fields)
          throws IOException, InterruptedException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(fields);
    }
    final String writeLine = writer.toString().trim();
    final Text csvText = new Text( writeLine );
    outputs.write(outputName, csvText, NullWritable.get(), outputName + "/" + outputName);
  }

  public void cleanup(final MultipleOutputs<Text, NullWritable> outputs) throws IOException, InterruptedException {
	  	outputs.close();
  }
}
