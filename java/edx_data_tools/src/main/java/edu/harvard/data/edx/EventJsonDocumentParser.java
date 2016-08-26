package edu.harvard.data.edx;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.edx.bindings.phase0.Phase0Event;
import edu.harvard.data.io.JsonDocumentParser;

public class EventJsonDocumentParser implements JsonDocumentParser {
  private static final Logger log = LogManager.getLogger();

  private final TableFormat format;
  private final boolean verify;

  public EventJsonDocumentParser(final TableFormat format, final boolean verify) {
    this.format = format;
    this.verify = verify;
  }

  @Override
  public Map<String, List<? extends DataTable>> getDocuments(final Map<String, Object> values)
      throws ParseException, VerificationException {
    final Map<String, List<? extends DataTable>> tables = new HashMap<String, List<? extends DataTable>>();
    final Phase0Event event = new Phase0Event(format, values);
    tables.put("event", Collections.singletonList(event));
    return tables;
  }

}
