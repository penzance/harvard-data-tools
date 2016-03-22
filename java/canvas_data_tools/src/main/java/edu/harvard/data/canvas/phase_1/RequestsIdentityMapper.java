package edu.harvard.data.canvas.phase_1;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityMap;

public class RequestsIdentityMapper
extends Mapper<Object, Text, LongWritable, HadoopIdentityKey> {

  protected final TableFormat format;

  public RequestsIdentityMapper() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0Requests request = new Phase0Requests(format, csvRecord);
      final Long canvasDataId = request.getUserId();
      if (canvasDataId != null) {
        final IdentityMap id = new IdentityMap();
        id.setCanvasDataId(canvasDataId);
        context.write(new LongWritable(canvasDataId), new HadoopIdentityKey(id));
      }
    }
  }
}
