package edu.harvard.data.identity;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Definition of the general (i.e. not table-specific) reducer job for a dataset
 * where the main identifier is typed as a Long. The identity map-reduce job
 * will consist of many mappers (one for each identifying table), but only one
 * reducer. The majority of the work for the reduce job is performed by
 * {@link IdentityReducer}; this class exists to satisfy the requirements of
 * Hadoop that reducers statically declare their key type in terms of
 * Hadoop-specific wrapper types
 */
public class LongIdentityReducer
extends Reducer<LongWritable, HadoopIdentityKey, Text, NullWritable> {

  private final IdentityReducer<Long> identityReducer;
  private MultipleOutputs<Text, NullWritable> outputs;

  public LongIdentityReducer() {
    this.identityReducer = new IdentityReducer<Long>();
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
  public void reduce(final LongWritable key, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final Long mainIdValue = key.get();
    identityReducer.reduce(mainIdValue, values, outputs);
  }
}
