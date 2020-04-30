package edu.harvard.data.zoom;

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
import edu.harvard.data.zoom.bindings.phase0.Phase0Meetings;
import edu.harvard.data.zoom.bindings.phase0.Phase0Activities;
import edu.harvard.data.zoom.bindings.phase0.Phase0Participants;
import edu.harvard.data.zoom.bindings.phase0.Phase0Quality;
import edu.harvard.data.zoom.bindings.phase0.Phase0Users;


public class EventJsonDocumentParser implements JsonDocumentParser {
  private static final Logger log = LogManager.getLogger();

  private final TableFormat format;
  private final boolean verify;
  private final String dataproduct;

  public EventJsonDocumentParser(final TableFormat format, final boolean verify, final String dataproduct) {
    this.format = format;
    this.verify = verify;
    this.dataproduct = dataproduct;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, List<? extends DataTable>> getDocuments(final Map<String, Object> values)
      throws ParseException, VerificationException {
    final Map<String, List<? extends DataTable>> tables = new HashMap<String, List<? extends DataTable>>();
    // Start
    
    // Meetings
    final Phase0Meetings meeting = new Phase0Meetings(format, values);
    final List<Phase0Meetings> meetings = new ArrayList<Phase0Meetings>();
    meetings.add(meeting);
    tables.put("Meetings", meetings );
    
    
    // Activities
    final Phase0Activities activity = new Phase0Activities(format, values);
    final List<Phase0Activities> activities = new ArrayList<Phase0Activities>();
    activities.add(activity);
    tables.put("Activities", activities );
    
    // Participants
    final Phase0Participants participant = new Phase0Participants(format, values);
    final List<Phase0Participants> participants = new ArrayList<Phase0Participants>();
    participants.add(participant);
    tables.put("Participants", participants );
    
    // Quality
    final Phase0Quality quality = new Phase0Quality(format, values);
    final List<Phase0Quality> qualities = new ArrayList<Phase0Quality>();
    qualities.add(quality);
    tables.put("Quality", qualities );
    
    // Users
    final Phase0Users user = new Phase0Users(format, values);
    final List<Phase0Users> users = new ArrayList<Phase0Users>();
    users.add(user);
    tables.put("Users", users);
    

    // Verification Step (optional)
    if (verify) {
    	verifyParser(values, tables);
    }
    return tables;
  }

  public void verifyParser(final Map<String, Object> values,
      final Map<String, List<? extends DataTable>> tables) throws VerificationException {
    // Start
	final List<? extends DataTable> meetings = tables.get("Meetings");
	final List<? extends DataTable> activities = tables.get("Activities");
	final List<? extends DataTable> participants = tables.get("Participants");
	final List<? extends DataTable> qualities = tables.get("Quality");
	final List<? extends DataTable> users = tables.get("Users");


	final Map<String, Object> parsedMeetings = meetings.get(0).getFieldsAsMap();
	final Map<String, Object> parsedActivities = activities.get(0).getFieldsAsMap();
	final Map<String, Object> parsedParticipants = participants.get(0).getFieldsAsMap();
	final Map<String, Object> parsedQualities = qualities.get(0).getFieldsAsMap();
	final Map<String, Object> parsedUsers = users.get(0).getFieldsAsMap();
	
	// Meetings
	if (dataproduct.equals("Meetings")) {
		
	    try {
		    compareMaps(values, parsedMeetings );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedMeetings );
	        throw e;			
	    }
	}
	else if (dataproduct.equals("Activities")) {
		
	    try {
		    compareMaps(values, parsedActivities );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedActivities );
	        throw e;			
	    }
	}
	else if (dataproduct.equals("Participants")) {
		
	    try {
		    compareMaps(values, parsedParticipants );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedParticipants );
	        throw e;			
	    }
	}
	else if (dataproduct.equals("Quality")) {
		
	    try {
		    compareMaps(values, parsedQualities );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedQualities );
	        throw e;			
	    }
	}
	else if (dataproduct.equals("Users")) {
		
	    try {
		    compareMaps(values, parsedUsers );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedUsers );
	        throw e;			
	    }
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
        final String v1 = cleanValue( m1.get(key).toString() );
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
          final String v2 = cleanValue( convertToString(m2.get(m2Key)) );
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

  private String cleanValue(String value) throws VerificationException {
	    if (value != null) {
	      value = value.replaceAll("\\t", " ");
	      value = value.replaceAll("\\n", "  ");
	    }
	    return value;
  }
  
  private String convertToString(final Object object) {
    if (object instanceof Timestamp) {
      return format.formatTimestamp(new Date(((Timestamp) object).getTime()));
    }
    return object.toString();
  }

}
