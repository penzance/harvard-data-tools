package edu.harvard.data.links;

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
import edu.harvard.data.links.bindings.phase0.Phase0ScraperText;
import edu.harvard.data.links.bindings.phase0.Phase0ScraperCitations;
import edu.harvard.data.links.bindings.phase0.Phase0ScraperCitationsCatalyst;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarMapping;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarBiblioTitles;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarPages;
import edu.harvard.data.links.bindings.phase0.Phase0Gazette;
import edu.harvard.data.links.bindings.phase0.Phase0GazetteEvents;
import edu.harvard.data.links.bindings.phase0.Phase0Rss;

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
    // Scraper 
    final Phase0ScraperText scraper = new Phase0ScraperText(format, values);
    final List<Phase0ScraperText> scraped = new ArrayList<Phase0ScraperText>();
    scraped.add(scraper);
    tables.put("ScraperText", scraped);

    // Citations
    final Phase0ScraperCitations citation = new Phase0ScraperCitations(format, values);
    final List<Phase0ScraperCitations> citations = new ArrayList<Phase0ScraperCitations>();
    citations.add(citation);
    tables.put("ScraperCitations", citations);
    
    // Catalyst
    final Phase0ScraperCitationsCatalyst catalyst = new Phase0ScraperCitationsCatalyst(format, values);
    final List<Phase0ScraperCitationsCatalyst> catalysts = new ArrayList<Phase0ScraperCitationsCatalyst>();
    catalysts.add(catalyst);
    tables.put("ScraperCitationsCatalyst", catalysts);
    
    // OpenScholarMapping
    final Phase0OpenScholarMapping osmapping = new Phase0OpenScholarMapping(format, values);
    final List<Phase0OpenScholarMapping> osmappings = new ArrayList<Phase0OpenScholarMapping>();
    osmappings.add(osmapping);
    tables.put("OpenScholarMapping", osmappings );
    
    // OpenScholarBiblioTitles
    final Phase0OpenScholarBiblioTitles ostitle = new Phase0OpenScholarBiblioTitles(format, values);
    final List<Phase0OpenScholarBiblioTitles> ostitles = new ArrayList<Phase0OpenScholarBiblioTitles>();
    ostitles.add(ostitle);
    tables.put("OpenScholarBiblioTitles", ostitles );
    
    // OpenScholarPages
    final Phase0OpenScholarPages ospage = new Phase0OpenScholarPages(format, values);
    final List<Phase0OpenScholarPages> ospages = new ArrayList<Phase0OpenScholarPages>();
    ospages.add(ospage);
    tables.put("OpenScholarPages", ospages );
    
    // Gazette
    final Phase0Gazette article = new Phase0Gazette(format, values);
    final List<Phase0Gazette> articles = new ArrayList<Phase0Gazette>();
    articles.add(article);
    tables.put("Gazette", articles );

    // Gazette Events
    final Phase0GazetteEvents gevent = new Phase0GazetteEvents(format, values);
    final List<Phase0GazetteEvents> gevents = new ArrayList<Phase0GazetteEvents>();
    gevents.add(gevent);
    tables.put("GazetteEvents", gevents );

    // RSS
    final Phase0Rss rssfeed = new Phase0Rss(format, values);
    final List<Phase0Rss> rssfeeds = new ArrayList<Phase0Rss>();
    rssfeeds.add(rssfeed);
    tables.put("Rss", rssfeeds );
    
    
    // Verification Step (optional)
    if (verify) {
    	verifyParser(values, tables);
    }
    return tables;
  }

  public void verifyParser(final Map<String, Object> values,
      final Map<String, List<? extends DataTable>> tables) throws VerificationException {
    // Start
	final List<? extends DataTable> scraped = tables.get("ScraperText");
	final List<? extends DataTable> citation = tables.get("ScraperCitations");
	final List<? extends DataTable> catalyst = tables.get("ScraperCitationsCatalyst");
	final List<? extends DataTable> osmapping = tables.get("OpenScholarMapping");
	final List<? extends DataTable> ostitles = tables.get("OpenScholarBiblioTitles");
	final List<? extends DataTable> ospages = tables.get("OpenScholarPages");
	final List<? extends DataTable> articles = tables.get("Gazette");
	final List<? extends DataTable> gevents = tables.get("GazetteEvents");
	final List<? extends DataTable> rssfeeds = tables.get("Rss");


	final Map<String, Object> parsedScraped = scraped.get(0).getFieldsAsMap();
	final Map<String, Object> parsedCitation = citation.get(0).getFieldsAsMap();
	final Map<String, Object> parsedCatalyst = catalyst.get(0).getFieldsAsMap();
	final Map<String, Object> parsedOSMapping = osmapping.get(0).getFieldsAsMap();
	final Map<String, Object> parsedOSTitles= ostitles.get(0).getFieldsAsMap();
	final Map<String, Object> parsedOSPages= ospages.get(0).getFieldsAsMap();
	final Map<String, Object> parsedArticles = articles.get(0).getFieldsAsMap();
	final Map<String, Object> parsedGevents = gevents.get(0).getFieldsAsMap();
	final Map<String, Object> parsedRssfeeds = rssfeeds.get(0).getFieldsAsMap();

	
	// Scraper
	if (dataproduct.equals("ScraperText")) {
		
	    try {
		    compareMaps(values, parsedScraped );
	    } catch (final VerificationException e) {
	        log.error("Failed to verify JSON document. " + e.getMessage());
	        log.error("Original map: " + values);
	        log.error("Parsed map:   " + parsedScraped );
	        throw e;			
	    }
	}
	// Citations
	else if (dataproduct.equals("ScraperCitations")) {
		
		try {	
			compareMaps(values, parsedCitation);		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedCitation);
		    throw e;				
		}
	}
	// Catalyst
	else if (dataproduct.equals("ScraperCitationsCatalyst")) {
		
		try {	
			compareMaps(values, parsedCatalyst);		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedCatalyst);
		    throw e;				
		}
	}
	// Open Scholar Mapping
	else if (dataproduct.equals("OpenScholarMapping")) {
		
		try {	
			compareMaps(values, parsedOSMapping );		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedOSMapping );
		    throw e;				
		}
	}
	// Open Scholar Biblio Titles
	else if (dataproduct.equals("OpenScholarBiblioTitles")) {
		
		try {	
			compareMaps(values, parsedOSTitles);		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedOSTitles );
		    throw e;				
		}
	}
	// Open Scholar Pages
	else if (dataproduct.equals("OpenScholarPages")) {
		
		try {	
			compareMaps(values, parsedOSPages );		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedOSPages );
		    throw e;				
		}
	}
	// Gazette
	else if (dataproduct.equals("Gazette")) {
		
		try {	
			compareMaps(values, parsedArticles );		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedArticles );
		    throw e;				
		}
	}
	// Gazette Events
	else if (dataproduct.equals("GazetteEvents")) {
		
		try {	
			compareMaps(values, parsedGevents );		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedGevents );
		    throw e;				
		}
	}
	// RSS
	else if (dataproduct.equals("Rss")) {
		
		try {	
			compareMaps(values, parsedRssfeeds );		
		} catch (final VerificationException e) {
		    log.error("Failed to verify JSON document. " + e.getMessage());
		    log.error("Original map: " + values);
		    log.error("Parsed map:   " + parsedRssfeeds );
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
