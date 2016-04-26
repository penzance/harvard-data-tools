package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.TableFormat;

/**
 * Definition of a map job that operates over records with a {@code Long} typed
 * main identifier. The majority of the work for this map job is performed by
 * {@link IdentityMapper}; this class exists to satisfy the requirements of
 * Hadoop that mappers statically declare their key type in terms of
 * Hadoop-specific wrapper types.
 */
public abstract class LongIdentityMapper extends
Mapper<Object, Text, LongWritable, HadoopIdentityKey> implements TableIdentityMapper<Long> {

  protected IdentityMapper<Long> mapper;
  protected TableFormat format;

  public LongIdentityMapper() {
    this.mapper = new IdentityMapper<Long>();
  }

  /**
   * Read and parse the {@link TableFormat} object that determines how records
   * are stored as strings. This value must be stored in the Hadoop context
   * under the key {@code "format"}.
   *
   * @throws InterruptedException
   *           see {link Mapper#setup}
   * @throws IOException
   *           see {link Mapper#setup}
   */
  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    mapper.setup(context);
  }

  /**
   * See {@link IdentityMapper#map(Text, TableIdentityMapper)}.
   */
  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final Map<Long, HadoopIdentityKey> map = mapper.map(value, this);
    for (final Long hadoopKey : map.keySet()) {
      final HadoopIdentityKey identityKey = map.get(hadoopKey);
      context.write(new LongWritable(hadoopKey), identityKey);
    }
  }
}
