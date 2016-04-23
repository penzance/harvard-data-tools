package edu.harvard.data.identity;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongIdentityReducer
extends Reducer<LongWritable, HadoopIdentityKey, Text, NullWritable> {

  private final IdentityReducer<Long> identityReducer;

  public LongIdentityReducer() {
    this.identityReducer = new IdentityReducer<Long>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    identityReducer.setup(context);
  }

  @Override
  public void reduce(final LongWritable key, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final Long mainIdValue = key.get();
    // TODO: Test that the reducer is passed the wrapped value, not the
    // LongWritable wrapper here.
    identityReducer.reduce(mainIdValue, values, context);
  }
}
