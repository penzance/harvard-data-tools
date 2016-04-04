package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.TableReader;

abstract class PreVerifyMapper extends Mapper<Object, Text, Text, LongWritable> {

  protected final Map<Long, IdentityMap> idByCanvasDataId;
  protected final TableFormat format;

  public PreVerifyMapper() {
    this.idByCanvasDataId = new HashMap<Long, IdentityMap>();
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (TableReader<IdentityMap> in = new FileTableReader<IdentityMap>(IdentityMap.class, format,
          fs.open(path));) {
        for (final IdentityMap id : in) {
          idByCanvasDataId.put(id.getCanvasDataID(), id);
        }
      }
    }
  }

}
