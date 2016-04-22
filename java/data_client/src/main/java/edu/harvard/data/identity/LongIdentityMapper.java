package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;

public abstract class LongIdentityMapper extends
Mapper<Object, Text, LongWritable, HadoopIdentityKey> implements GeneratedIdentityMapper<Long> {

  protected IdentityMapper<Long> mapper;
  protected TableFormat format;

  @Override
  protected void setup(final Context context) {
    this.format = HadoopJob.getFormat(context);
    mapper = new IdentityMapper<Long>(format);
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
