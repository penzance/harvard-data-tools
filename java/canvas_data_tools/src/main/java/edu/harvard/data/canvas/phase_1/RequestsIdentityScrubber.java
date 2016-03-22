package edu.harvard.data.canvas.phase_1;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;

import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;
import edu.harvard.data.canvas.bindings.phase1.Phase1Requests;
import edu.harvard.data.identity.IdentityScrubber;

public class RequestsIdentityScrubber extends IdentityScrubber {

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0Requests phase0 = new Phase0Requests(format, csvRecord);
      final Phase1Requests phase1 = new Phase1Requests(phase0);
      if (phase0.getUserId() != null) {
        phase1.setResearchUuid(identities.get(phase0.getUserId()).getResearchId());
      }
      writeRecord(phase1, context);
    }
  }
}
