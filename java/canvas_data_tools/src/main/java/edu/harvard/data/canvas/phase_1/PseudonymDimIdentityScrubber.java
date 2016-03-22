package edu.harvard.data.canvas.phase_1;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;

import edu.harvard.data.canvas.bindings.phase0.Phase0PseudonymDim;
import edu.harvard.data.canvas.bindings.phase1.Phase1PseudonymDim;
import edu.harvard.data.identity.IdentityScrubber;

public class PseudonymDimIdentityScrubber extends IdentityScrubber {

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0PseudonymDim phase0 = new Phase0PseudonymDim(format, csvRecord);
      final Phase1PseudonymDim phase1 = new Phase1PseudonymDim(phase0);
      if (phase0.getUserId() != null) {
        phase1.setResearchUuid(identities.get(phase0.getUserId()).getResearchId());
      }
      writeRecord(phase1, context);
    }
  }
}
