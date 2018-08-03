package edu.harvard.data.canvasrest;

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
import edu.harvard.data.canvasrest.bindings.phase0.Phase0Syllabus;
import edu.harvard.data.canvasrest.bindings.phase0.Phase0SyllabusBody;
import edu.harvard.data.canvasrest.bindings.phase0.Phase0SyllabusLink;
import edu.harvard.data.canvasrest.bindings.phase0.Phase0SyllabusNameLookup;
import edu.harvard.data.canvasrest.bindings.phase0.Phase0SyllabusDelta;
import edu.harvard.data.canvasrest.bindings.phase0.Phase0SyllabusFiles;

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
    // Syllabus
    final Phase0Syllabus syllabus = new Phase0Syllabus(format, values);
    final List<Phase0Syllabus> syllabi = new ArrayList<Phase0Syllabus>();
    syllabi.add(syllabus);
    tables.put("Syllabus", syllabi);

    // SyllabusBody
    final Phase0SyllabusBody syllabusbody = new Phase0SyllabusBody(format, values);
    final List<Phase0SyllabusBody> syllabusbodys = new ArrayList<Phase0SyllabusBody>();
    syllabusbodys.add(syllabusbody);
    tables.put("SyllabusBody", syllabusbodys);

    // SyllabusLink
    final Phase0SyllabusLink syllink = new Phase0SyllabusLink(format, values);
    final List<Phase0SyllabusLink> syllinks = new ArrayList<Phase0SyllabusLink>();
    syllinks.add(syllink);
    tables.put("SyllabusLink", syllinks);    
    
    // Syllabus Namelookup
    final Phase0SyllabusNameLookup sylnamelookup = new Phase0SyllabusNameLookup(format, values);
    final List<Phase0SyllabusNameLookup> sylnamelookups = new ArrayList<Phase0SyllabusNameLookup>();
    sylnamelookups.add(sylnamelookup);
    tables.put("SyllabusNameLookup", sylnamelookups);

    // Syllabus Delta
    final Phase0SyllabusDelta syllabusdelta = new Phase0SyllabusDelta(format, values);
    final List<Phase0SyllabusDelta> syllabusdeltas = new ArrayList<Phase0SyllabusDelta>();
    syllabusdeltas.add(syllabusdelta);
    tables.put("SyllabusDelta", syllabusdeltas);    

    // Syllabus Files
    final Phase0SyllabusFiles syllabusfile = new Phase0SyllabusFiles(format, values);
    final List<Phase0SyllabusFiles> syllabusfiles = new ArrayList<Phase0SyllabusFiles>();
    syllabusfiles.add(syllabusfile);
    tables.put("SyllabusFiles", syllabusfiles);     
    
    // Verification Step (optional)
    if (verify) {
    	verifyParser(values, tables);
    }
    return tables;
  }

  public void verifyParser(final Map<String, Object> values,
      final Map<String, List<? extends DataTable>> tables) throws VerificationException {
    // Start
	final List<? extends DataTable> syllabi = tables.get("Syllabus");
	final List<? extends DataTable> sylbody = tables.get("SyllabusBody");
	final List<? extends DataTable> syllink = tables.get("SyllabusLink");
	final List<? extends DataTable> sylnamelookup = tables.get("SyllabusNameLookup");
	final List<? extends DataTable> syldelta = tables.get("SyllabusDelta");
	final List<? extends DataTable> sylfiles = tables.get("SyllabusFiles");


	final Map<String, Object> parsedSyllabi = syllabi.get(0).getFieldsAsMap();
	final Map<String, Object> parsedSylbody = sylbody.get(0).getFieldsAsMap();
	final Map<String, Object> parsedSyllink = syllink.get(0).getFieldsAsMap();
	final Map<String, Object> parsedSylnamelookup = sylnamelookup.get(0).getFieldsAsMap();
	final Map<String, Object> parsedSyldelta = syldelta.get(0).getFieldsAsMap();
	final Map<String, Object> parsedSylfiles = sylfiles.get(0).getFieldsAsMap();

	
	// Syllabi
	if (dataproduct.equals("Syllabus")) {
		
	    try {
		    compareMaps(values, parsedSyllabi);
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedSyllabi);
	        throw e;			
	    }
	}
	// Syllabus Body
	else if (dataproduct.equals("SyllabusBody")) {
		
		try {	
			compareMaps(values, parsedSylbody);		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedSylbody);
		    throw e;				
		}
	}
	// SyllabusLink
	else if (dataproduct.equals("SyllabusLink")) {
		
		try {
			compareMaps(values, parsedSyllink);
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedSyllink);
		    throw e;
		}
	}
	// Syllabus Name Lookup
	else if (dataproduct.equals("SyllabusNameLookup")) {

		try {
			compareMaps(values, parsedSylnamelookup);
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedSylnamelookup);
		    throw e;			
		}
	}
	// Syllabus Delta
	else if (dataproduct.equals("SyllabusDelta")) {

		try {
			compareMaps(values, parsedSyldelta);
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedSyldelta);
		    throw e;			
		}
	}	
	// Syllabus Files
	else if (dataproduct.equals("SyllabusFiles")) {

		try {
			compareMaps(values, parsedSylfiles);
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedSylfiles);
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
	      value = value.replaceAll("\t", "\\t");
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
