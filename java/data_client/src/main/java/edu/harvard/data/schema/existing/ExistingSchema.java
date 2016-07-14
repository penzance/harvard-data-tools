package edu.harvard.data.schema.existing;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.generator.CodeGenerator;

public class ExistingSchema {

  private static final Logger log = LogManager.getLogger();

  private final Map<String, ExistingSchemaTable> tables;

  @JsonCreator
  public ExistingSchema(@JsonProperty("tables") final Map<String, ExistingSchemaTable> tableMap) {
    this.tables = new HashMap<String, ExistingSchemaTable>();
    if (tableMap != null) {
      for (final String key : tableMap.keySet()) {
        tables.put(key, tableMap.get(key));
      }
    }
  }

  public Map<String, ExistingSchemaTable> getTables() {
    return tables;
  }

  public static ExistingSchema readExistingSchemas(final String jsonResource) throws IOException {
    log.info("Reading existing table schemas from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      final ExistingSchema schema = jsonMapper.readValue(in, ExistingSchema.class);
      for (final String tableName : schema.getTables().keySet()) {
        schema.getTables().get(tableName).setTableName(tableName);
      }
      return schema;
    }
  }
}
