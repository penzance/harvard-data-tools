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
import edu.harvard.data.matterhorn.bindings.phase0.Phase0GeoIp;
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
    if (values.containsKey("geoip") && ((Map<String, Object>) values.get("geoip")).size() > 0) {
      final Map<String, Object> fields = (Map<String, Object>) values.get("geoip");
      fields.remove("location"); // Location is redundant, and typed as a list
      final Phase0GeoIp geoip = new Phase0GeoIp(format, fields);
      final List<Phase0GeoIp> geoips = new ArrayList<Phase0GeoIp>();
      geoips.add(geoip);
      tables.put("geo_ip", geoips);
    }
    final List<Phase0Event> events = new ArrayList<Phase0Event>();
    events.add(event);
    tables.put("event", events);
    if (verify) {
      verifyParser(values, tables);
    }
    return tables;
  }

  public void verifyParser(final Map<String, Object> values,
      final Map<String, List<? extends DataTable>> tables) throws VerificationException {
    values.remove("_meta");
    values.remove("useragent");
    final List<? extends DataTable> events = tables.get("event");
    final List<? extends DataTable> videos = tables.get("video");
    final List<? extends DataTable> geoips = tables.get("geo_ip");
    final Map<String, Object> parsed = events.get(0).getFieldsAsMap();
    if (videos != null && !videos.isEmpty()) {
      parsed.put("episode", videos.get(0).getFieldsAsMap());
    }
    if (geoips != null && !geoips.isEmpty()) {
      parsed.put("geoip", geoips.get(0).getFieldsAsMap());
    }
    try {
      if (values.containsKey("geoip") && ((Map<?,?>)values.get("geoip")).isEmpty()) {
        values.remove("geoip");
      }
      if (values.containsKey("episode") && ((Map<?,?>)values.get("episode")).isEmpty()) {
        values.remove("episode");
      }
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
      String m2Key = key;
      if (key.startsWith("@")) {
        m2Key = key.substring(1);
      }
      if (!m2.containsKey(m2Key)) {
        throw new VerificationException("Missing key: " + m2Key);
      }
      if (m1.get(key) == null && m2.get(m2Key) != null) {
        throw new VerificationException("Key " + key + " should be null, not " + m2.get(m2Key));
      } else if (m1.get(key) instanceof Map) {
        if (!(m2.get(m2Key) instanceof Map)) {
          throw new VerificationException("Incorrect type for key " + key);
        }
        compareMaps((Map<String, Object>) m1.get(key), (Map<String, Object>) m2.get(m2Key));
      } else {
        final String v1 = m1.get(key).toString();
        if (m2.get(m2Key) == null) {
          throw new VerificationException("Key " + key + " should not be null");
        }
        if (m2.get(m2Key) instanceof Boolean) {
          if ((boolean) m2.get(m2Key)) {
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
          final String v2 = convertToString(m2.get(m2Key));
          if (m2.get(m2Key) instanceof Timestamp) {
            compareTimestamps(v1, v2, key);
          } else if (m2.get(m2Key) instanceof Double) {
            compareDoubles(v1, v2, key);
          } else {
            if (!v1.equals(v2)) {
              throw new VerificationException(
                  "Different values for key " + key + ". Original: " + v1 + ", new: " + v2);
            }
          }
        }
      }
    }
  }

  private void compareDoubles(String v1, String v2, final String key) throws VerificationException {
    if (v1.endsWith(".0")) {
      v1 = v1.substring(0, v1.lastIndexOf("."));
    }
    if (v2.endsWith(".0")) {
      v2 = v2.substring(0, v2.lastIndexOf("."));
    }
    if (!v1.equals(v2)) {
      throw new VerificationException(
          "Different values for key " + key + ". Original: " + v1 + ", new: " + v2);
    }
  }

  private void compareTimestamps(String v1, String v2, final String key)
      throws VerificationException {
    if (v1.endsWith(".000Z")) {
      v1 = v1.substring(0, v1.lastIndexOf(".")) + "Z";
    }
    if (v2.endsWith(".000Z")) {
      v2 = v2.substring(0, v2.lastIndexOf(".")) + "Z";
    }
    if (!v1.equals(v2)) {
      throw new VerificationException(
          "Different values for key " + key + ". Original: " + v1 + ", new: " + v2);
    }
  }

  private String convertToString(final Object object) {
    if (object instanceof Timestamp) {
      return format.formatTimestamp(new Date(((Timestamp) object).getTime()));
    }
    return object.toString();
  }

}
