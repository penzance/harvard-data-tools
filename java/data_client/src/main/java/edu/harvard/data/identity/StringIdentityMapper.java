package edu.harvard.data.identity;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;

/**
 * Definition of a map job that operates over records with a {@code String}
 * typed main identifier. The majority of the work for this map job is performed
 * by {@link IdentityMapper}; this class exists to satisfy the requirements of
 * Hadoop that mappers statically declare their key type in terms of
 * Hadoop-specific wrapper types.
 */
public abstract class StringIdentityMapper extends Mapper<Object, Text, Text, HadoopIdentityKey>
implements TableIdentityMapper<String> {

  protected IdentityMapper<String> mapper;
  protected TableFormat format;

  /**
   * Read and parse the {@link TableFormat} object that determines how records
   * are stored as strings. This value must be stored in the Hadoop context
   * under the key {@code "format"}.
   */
  @Override
  protected void setup(final Context context) {
    this.format = HadoopJob.getFormat(context);
    mapper = new IdentityMapper<String>(format);
  }

  /**
   * See {@link IdentityMapper#map(Text, TableIdentityMapper)}.
   */
  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final Map<String, HadoopIdentityKey> map = mapper.map(value, this);
    for (final String hadoopKey : map.keySet()) {
      final HadoopIdentityKey identityKey = map.get(hadoopKey);
      context.write(new Text(hadoopKey), identityKey);
    }
  }
}