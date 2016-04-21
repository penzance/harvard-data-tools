package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.TableFormat;

public abstract class StringIdentityMapper extends
Mapper<Object, Text, Text, HadoopIdentityKey> implements GeneratedIdentityMapper<String> {

  protected TableFormat format;
  private final IdentityMapper<String> mapper;

  public StringIdentityMapper() {
    mapper = new IdentityMapper<String>();
  }

  @Override
  protected void setup(final Context context) {
    mapper.setup(context);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final Map<String, HadoopIdentityKey> map = mapper.map(key, value, context, this);
    for (final String hadoopKey : map.keySet()) {
      final HadoopIdentityKey identityKey = map.get(hadoopKey);
      context.write(new Text(hadoopKey), identityKey);
    }
  }
}