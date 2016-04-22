package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class LongIdentityMapper extends
Mapper<Object, Text, LongWritable, HadoopIdentityKey> implements GeneratedIdentityMapper<Long> {

  protected final IdentityMapper<Long> mapper;

  public LongIdentityMapper() {
    mapper = new IdentityMapper<Long>();
  }

  @Override
  protected void setup(final Context context) {
    mapper.setup(context);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final Map<Long, HadoopIdentityKey> map = mapper.map(value, context, this);
    for (final Long hadoopKey : map.keySet()) {
      final HadoopIdentityKey identityKey = map.get(hadoopKey);
      context.write(new LongWritable(hadoopKey), identityKey);
    }
  }
}
