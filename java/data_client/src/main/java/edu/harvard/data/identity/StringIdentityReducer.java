package edu.harvard.data.identity;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Definition of the general (i.e. not table-specific) reducer job for a dataset
 * where the main identifier is typed as a String. The identity map-reduce job
 * will consist of many mappers (one for each identifying table), but only one
 * reducer. The majority of the work for the reduce job is performed by
 * {@link IdentityReducer}; this class exists to satisfy the requirements of
 * Hadoop that reducers statically declare their key type in terms of
 * Hadoop-specific wrapper types
 */
public class StringIdentityReducer extends Reducer<Text, HadoopIdentityKey, Text, NullWritable> {

  private final IdentityReducer<String> identityReducer;
  private MultipleOutputs<Text, NullWritable> outputs;

  public StringIdentityReducer() {
    this.identityReducer = new IdentityReducer<String>();
  }

  /**
   * Perform initialization tasks for the job. See the definition of
   * {@link IdentityReducer#setup} for details.
   */
  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    identityReducer.setup(context);
    outputs = new MultipleOutputs<Text, NullWritable>(context);
  }

  /**
   * Process the identity records associated with a single main identifier
   * value. See the definition of {@link IdentityReducer#reduce} for details.
   */
  @Override
  public void reduce(final Text key, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final String mainIdValue = key.toString();
    identityReducer.reduce(mainIdValue, values, outputs);
  }
  
  /**
   * Cleanup the identity records associated with a single main identifier
   * value and ensure files are closed. See the definition of {@link IdentityReducer#cleanup} for details.
   */
  @Override
  public void cleanup( final Context context ) throws IOException, InterruptedException {
    identityReducer.cleanup( outputs );
  }
  
}
