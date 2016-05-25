package edu.harvard.data.io;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import edu.harvard.data.DataTable;
import edu.harvard.data.VerificationException;

/**
 * Interface required by the {@link JsonFileIterator} class to extract all
 * {@link DataTable} records that are contained in a JSON document. A custom
 * subclass of this interface should be implemented by any client that need to
 * parse JSON-formatted data.
 */
public interface JsonDocumentParser {

  /**
   * Extract all {@link DataTable} records from a parsed JSON object.
   *
   * @param values
   *          the data parsed from an input JSON string. This data is typed as a
   *          {@link Map} from JSON key to an {@code Object} representing its
   *          value. The value may be of any type; it is assumed that the
   *          implementation of this interface will understand the structure of
   *          a JSON object being parsed, and so should be able to cast the
   *          {@code Object} to the appropriate types.
   *
   * @return a {@link Map} from data table name to a {@link List} of
   *         {@link DataTable} objects that represent all records for that table
   *         that can be extracted from the JSON document. The return value must
   *         not be null; if there are no records to extract, this method must
   *         return an empty map.
   *
   * @throws ParseException
   *           if some field in the JSON document cannot be parsed.
   * @throws VerificationException
   *           if the data contained in the JSON document violates some
   *           implementation-specific verification criteria.
   */
  Map<String, List<? extends DataTable>> getDocuments(Map<String, Object> values)
      throws ParseException, VerificationException;
}
