package edu.harvard.data.schema.identity;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.identity.IdentifierType;

/**
 * The Identity Schema defines the set of user identifiers in a data set. It is
 * generally specified through a JSON object with the following structure:
 *
 * <pre>
 * <code>
 * { "table_name" : {
 *     "identifying_column_name" : ["IdentityType"],
 *     "multiplexed_column_name" : ["IdentityType1", "IdentityType2"],
 *     },
 * { "table_name2" : {
 *     "identifying_column_name" : ["IdentityType"],
 *     "confidential_column_name" : ["Other"]
 *     }
 * }
 * </code>
 * </pre>
 *
 * For example, the Canvas Data identity specification contains the following
 * table declaration:
 *
 * <pre>
 * <code>
 * "pseudonym_dim": {
 *   "user_id": ["CanvasDataID"],
 *   "canvas_id": ["CanvasID"],
 *   "sis_user_id": ["HUID", "XID"],
 *   "unique_name": ["Other"]
 *   "id": ["Other"],
 * },
 * </code>
 * </pre>
 *
 * In this table (<code>pseudonym_dim</code>), we see examples of several
 * different identifying columns. The <code>user_id</code> column contains the
 * {@link IdentifierType#CanvasDataID} for this individual (this is the main
 * identifier for the Canvas Data set). The table also has second identifier of
 * type {@link IdentifierType#CanvasID} which, although it is not the main
 * identifer in the data set is still a unique identifier for this individual.
 * In this case, the Canvas ID can be used to look up an individual's account on
 * the Canvas platform.
 * <p>
 * The field <code>sis_user_id</code> contains a key into an external Student
 * Information System, which was fed into Canvas from an on-campus source. In
 * Harvard's case, this column could contain either an official Harvard ID (an
 * {@link IdentifierType#HUID}), or a secondary {@link IdentifierType#XID},
 * typically given to guests or other associated individuals. Since two
 * identifier types are multiplexed into a single column, we can rely on the
 * regular expressions specified by {@link IdentifierType#getPattern} to
 * determine which is which.
 * <p>
 * Finally, the <code>unique_name</code> and <code>id</code> columns contain
 * information which is confidential but does not contribute to the referential
 * integrity of the data set (for example, the <code>unique_name</code> column
 * contains a user's e-mail address in Harvard's instance of the Canvas Data
 * set). In this case we specify the fields as {@link IdentifierType#Other} to
 * indicate that they should be scrubbed by the identity phase of the data
 * pipeline, but that they should not be replaced by anything.
 * <p>
 * The resulting schema produced by this transformer for the
 * <code>pseudonym_dim</code> table is almost identical to the input schema, but
 * has the following differences:
 * <ul>
 * <li>The five fields <code>user_id</code>, <code>canvas_id</code>,
 * <code>sis_user_id</code>, <code>unique_name</code> and <code>id</code> have
 * been removed.
 * <li>Three new fields, each typed as {@code String} have been added:
 * <code>user_id_research_uuid</code>, <code>canvas_id_research_uuid</code> and
 * <code>sis_user_id_research_uuid</code>.
 * </ul>
 * <p>
 * We add a research UUID for each field even though, in this case, they all
 * hold the same value. In some cases the research UUID values will differ
 * between fields. For example, the Canvas Data table
 * <code>submission_dim</code> has this identifier specification:
 *
 * <pre>
 * <code>
 * "submission_dim": {
 *   "user_id": ["CanvasDataID"],
 *   "grader_id": ["CanvasDataID"]
 * },
 * </code>
 * </pre>
 *
 * In this case, there are {@code CanvasDataID} values for two unique
 * individuals; the student who submitted an assignment, and the individual who
 * graded that assignment. When we have multiple instances of the data set's
 * main identifier in a single table, we need to retain the identities of both
 * individuals, and so here we would see different values for
 * <code>user_id_research_uuid</code> and <code>grader_id_research_uuid</code>.
 */
public class IdentitySchema {

  private static final Logger log = LogManager.getLogger();

  public Map<String, Map<String, List<IdentifierType>>> tables;

  public IdentitySchema(final Map<String, Map<String, List<IdentifierType>>> tableMap) {
    this.tables = new HashMap<String, Map<String, List<IdentifierType>>>();
    if (tableMap != null) {
      for (final String key : tableMap.keySet()) {
        tables.put(key, tableMap.get(key));
      }
    }
  }

  /**
   * Get the names of all tables that have identifiers as specified by this
   * schema.
   *
   * @return a {@link Set} of table names.
   */
  public Set<String> tableNames() {
    return new HashSet<String>(tables.keySet());
  }

  /**
   * Get the identifier types for a single table as defined by this schema.
   *
   * @param tableName
   *          the name of the table for which to find identifiers.
   *
   * @return a {@link Map} from identifier column name to a {@link List} of
   *         {@link IdentifierType} values. The identifiers are returned as a
   *         {@code List} because multiple identifiers may be multiplexed into a
   *         single column in a data set.
   */
  public Map<String, List<IdentifierType>> get(final String tableName) {
    return tables.get(tableName);
  }

  /**
   * Parse a JSON document to extract an IdentitySchema. The JSON document is
   * specified as a Java classpath resource name, and must be formatted as
   * described in this class description.
   *
   * @param jsonResource
   *          the name of a resource containing a well-formed JSON document
   *          describing an identity schema. The resource must be accessible to
   *          the class loader that loaded this class.
   * @return an {@code} IdentitySchema containing the data defined in the JSON
   *         resource.
   *
   * @throws IOException
   *           if and error occurs when reading or parsing the JSON resource.
   */
  @SuppressWarnings("unchecked")
  public static IdentitySchema read(final String jsonResource) throws IOException {
    log.info("Reading identifiers from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    final TypeReference<Map<String, Map<String, List<IdentifierType>>>> identiferTypeRef = new TypeReference<Map<String, Map<String, List<IdentifierType>>>>() {
    };
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      return new IdentitySchema((Map<String, Map<String, List<IdentifierType>>>) jsonMapper
          .readValue(in, identiferTypeRef));
    }
  }
}
