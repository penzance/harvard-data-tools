package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.TableFormat;
import edu.harvard.data.io.FileTableReader;

public abstract class IdentityMapper
extends Mapper<Object, Text, LongWritable, HadoopIdentityKey> {

  protected final TableFormat format;
  protected final Map<Long, IdentityMap> knownIdentities;

  public IdentityMapper(final TableFormat format) {
    this.format = format;
    this.knownIdentities = new HashMap<Long, IdentityMap>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (final FSDataInputStream inStream = fs.open(path);
          FileTableReader<IdentityMap> in = new FileTableReader<IdentityMap>(IdentityMap.class,
              format, inStream)) {
        for (final IdentityMap id : in) {
          knownIdentities.put(id.getCanvasDataId(), id);
        }
      }
    }
  }
}
