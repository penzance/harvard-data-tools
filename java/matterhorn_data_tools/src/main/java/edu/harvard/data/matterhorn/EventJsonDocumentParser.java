package edu.harvard.data.matterhorn;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.io.JsonDocumentParser;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Event;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Video;

public class EventJsonDocumentParser implements JsonDocumentParser {
  private static final Logger log = LogManager.getLogger();

  private final TableFormat format;
  private final boolean verify;

  public EventJsonDocumentParser(final TableFormat format, final boolean verify) {
    this.format = format;
    this.verify = verify;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, List<? extends DataTable>> getDocuments(final Map<String, Object> values)
      throws ParseException, VerificationException {
    final Map<String, List<? extends DataTable>> tables = new HashMap<String, List<? extends DataTable>>();
    final Phase0Event event = new Phase0Event(format, values);
    if (values.containsKey("episode") && ((Map<String, Object>) values.get("episode")).size() > 0) {
      final Map<String, Object> fields = (Map<String, Object>) values.get("episode");
      final Phase0Video video = new Phase0Video(format, fields);
      video.setId(event.getMpid());
      final List<Phase0Video> videos = new ArrayList<Phase0Video>();
      videos.add(video);
      tables.put("video", videos);
    }
    final List<Phase0Event> events = new ArrayList<Phase0Event>();
    events.add(event);
    tables.put("event", events);
    if (verify) {
      verifyParser(values, tables);
    }
    return tables;
  }

  public void verifyParser(final Map<String, Object> values, final Map<String, List<? extends DataTable>> tables)
      throws VerificationException {
    final List<? extends DataTable> events = tables.get("event");
    final List<? extends DataTable> videos = tables.get("video");
    final Map<String, Object> parsed = events.get(0).getFieldsAsMap();
    if (videos != null && !videos.isEmpty()) {
      parsed.put("episode", videos.get(0).getFieldsAsMap());
    }
    try {
      compareMaps(values, parsed);
    } catch (final VerificationException e) {
      log.error("Failed to verify JSON document. " + e.getMessage());
      log.error("Original map: " + values);
      log.error("Parsed map:   " + parsed);
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private void compareMaps(final Map<String, Object> m1, final Map<String, Object> m2)
      throws VerificationException {
    for (final String key : m1.keySet()) {
      if (!m2.containsKey(key)) {
        throw new VerificationException("Missing key: " + key);
      }
      if (m1.get(key) == null && m2.get(key) != null) {
        throw new VerificationException("Key " + key + " should be null, not " + m2.get(key));
      } else if (m1.get(key) instanceof Map) {
        if (!(m2.get(key) instanceof Map)) {
          throw new VerificationException("Incorrect type for key " + key);
        }
        compareMaps((Map<String, Object>) m1.get(key), (Map<String, Object>) m2.get(key));
      } else {
        final String v1 = m1.get(key).toString();
        if (m2.get(key) == null) {
          throw new VerificationException("Key " + key + " should not be null");
        }
        if (m2.get(key) instanceof Boolean) {
          if ((boolean) m2.get(key)) {
            if (!(v1.equals("true") || v1.equals("1"))) {
              throw new VerificationException(
                  "Different values for key " + key + ". Original: " + v1 + ", new: true");
            }
          } else {
            if (!(v1.equals("false") || v1.equals("0"))) {
              throw new VerificationException(
                  "Different values for key " + key + ". Original: " + v1 + ", new: false");
            }
          }
        } else {
          final String v2 = convertToString(m2.get(key));
          if (!v1.equals(v2)) {
            throw new VerificationException(
                "Different values for key " + key + ". Original: " + v1 + ", new: " + v2);
          }
        }
      }
    }
  }

  private String convertToString(final Object object) {
    if (object instanceof Timestamp) {
      return format.formatTimestamp(new Date(((Timestamp) object).getTime()));
    }
    return object.toString();
  }

}
