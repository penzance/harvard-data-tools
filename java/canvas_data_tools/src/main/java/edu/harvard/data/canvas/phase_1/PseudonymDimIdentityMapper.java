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
import edu.harvard.data.canvas.bindings.phase0.Phase0PseudonymDim;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;

public class PseudonymDimIdentityMapper
extends Mapper<Object, Text, LongWritable, HadoopIdentityKey> {

  protected final TableFormat format;

  public PseudonymDimIdentityMapper() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0PseudonymDim phase0 = new Phase0PseudonymDim(format, csvRecord);
      final Long canvasDataId = phase0.getUserId();

      boolean populated = false;
      final IdentityMap id = new IdentityMap();
      if (phase0.getUserId() != null) {
        id.setCanvasDataId(phase0.getUserId());
        populated = true;
      }
      if (phase0.getCanvasId() != null) {
        id.setCanvasId(phase0.getCanvasId());
        populated = true;
      }
      if (phase0.getSisUserId() != null) {
        final String sisUserId = phase0.getSisUserId();
        if (IdentifierType.HUID.getPattern().matcher(sisUserId).matches()) {
          id.setHuid(sisUserId);
          populated = true;
        }
        if (IdentifierType.XID.getPattern().matcher(sisUserId).matches()) {
          id.setXid(sisUserId);
          populated = true;
        }
      }
      if (populated) {
        context.write(new LongWritable(canvasDataId), new HadoopIdentityKey(id));
      }
    }
  }
}
