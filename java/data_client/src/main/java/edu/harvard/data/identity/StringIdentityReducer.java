package edu.harvard.data.identity;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StringIdentityReducer extends Reducer<Text, HadoopIdentityKey, Text, NullWritable> {

  private final IdentityReducer<String> identityReducer;

  public StringIdentityReducer(final IdentifierType mainIdentifier) {
    this.identityReducer = new IdentityReducer<String>(mainIdentifier);
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    identityReducer.setup(context);
  }

  @Override
  public void reduce(final Text key, final Iterable<HadoopIdentityKey> values,
      final Context context) throws IOException, InterruptedException {
    final String mainIdValue = key.toString();
    identityReducer.reduce(mainIdValue, values, context);
  }
}
