package edu.harvard.data.schema.fulltext;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.generator.CodeGenerator;

public class FullTextSchema {

  private static final Logger log = LogManager.getLogger();

  public Map<String, FullTextTable> tables;

  public FullTextSchema() {
    this.tables = new HashMap<String, FullTextTable>();
  }

  @JsonCreator
  public FullTextSchema(@JsonProperty("tables") final Map<String, FullTextTable> tableMap) {
    this();
    if (tableMap != null) {
      tables.putAll(tableMap);
    }
  }

  /**
   * Get the names of all tables that have columns containing free text as
   * specified by this schema.
   *
   * @return a {@link Set} of table names.
   */
  public Set<String> tableNames() {
    return new HashSet<String>(tables.keySet());
  }

  /**
   * Get the columns with free text for a single table as defined by this
   * schema.
   *
   * @param tableName
   *          the name of the table for which to find identifiers.
   *
   * @return a <@link List> of column names.
   */
  public FullTextTable get(final String tableName) {
    return tables.get(tableName);
  }

  /**
   * Parse a JSON document to extract a FullTextSchema. The JSON document is
   * specified as a Java classpath resource name, and must be formatted as
   * described in this class description.
   *
   * @param jsonResource
   *          the name of a resource containing a well-formed JSON document
   *          describing an full text schema. The resource must be accessible to
   *          the class loader that loaded this class.
   * @return a {@code FullTextSchema} containing the data defined in the JSON
   *         resource.
   *
   * @throws IOException
   *           if and error occurs when reading or parsing the JSON resource.
   */
  public static FullTextSchema read(final String jsonResource) throws IOException {
    if (jsonResource != null) {
      log.info("Reading identifiers from file " + jsonResource);
      final ObjectMapper jsonMapper = new ObjectMapper();
      jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
      final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
      try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
        return jsonMapper.readValue(in, FullTextSchema.class);
      }
    } else {
      return new FullTextSchema();
    }
  }
}
