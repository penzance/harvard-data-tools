package edu.harvard.data.matterhorn.phase_1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.TableFormat;
import edu.harvard.data.identity.IdentityMap;

abstract class PreVerifyMapper extends Mapper<Object, Text, Text, LongWritable> {

  protected final Map<Long, IdentityMap> idByCanvasDataId;
  protected TableFormat format;

  public PreVerifyMapper() {
    this.idByCanvasDataId = new HashMap<Long, IdentityMap>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
  }

}
