package edu.harvard.data.schema.extension;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.CodeGenerator;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;

public class ExtensionSchema implements DataSchema {

  private static final Logger log = LogManager.getLogger();

  private final Map<String, DataSchemaTable> tables;

  @JsonCreator
  public ExtensionSchema(@JsonProperty("tables") final Map<String, ExtensionSchemaTable> tableMap) {
    this.tables = new HashMap<String, DataSchemaTable>();
    if (tableMap != null) {
      for (final String key : tableMap.keySet()) {
        tables.put(key, tableMap.get(key));
      }
    }
  }

  private ExtensionSchema(final ExtensionSchema original) {
    this.tables = new HashMap<String, DataSchemaTable>();
    for (final String key : original.tables.keySet()) {
      tables.put(key, original.tables.get(key).copy());
    }
  }

  @Override
  public Map<String, DataSchemaTable> getTables() {
    return tables;
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public DataSchema copy() {
    return new ExtensionSchema(this);
  }

  @Override
  public DataSchemaTable getTableByName(final String name) {
    return tables.get(name);
  }

  @Override
  public void addTable(final String tableName, final DataSchemaTable newTable) {
    tables.put(tableName, newTable);
  }

  @Override
  public String toString() {
    String s = "";
    final List<String> tableNames = new ArrayList<String>(tables.keySet());
    Collections.sort(tableNames);
    for (final String t : tableNames) {
      s += tables.get(t) + "\n";
    }
    return s.trim();
  }

  public static ExtensionSchema readExtensionSchema(final String jsonResource)
      throws IOException, VerificationException {
    log.info("Extending schema from file " + jsonResource);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(new SimpleDateFormat(FormatLibrary.JSON_DATE_FORMAT_STRING));
    final ClassLoader classLoader = CodeGenerator.class.getClassLoader();
    final ExtensionSchema schema;
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      schema = jsonMapper.readValue(in, ExtensionSchema.class);
    }
    for (final String tableName : schema.getTables().keySet()) {
      ((ExtensionSchemaTable) schema.getTables().get(tableName)).setTableName(tableName);
    }
    verify(schema);
    return schema;
  }

  private static void verify(final ExtensionSchema schema) throws VerificationException {
    for (final DataSchemaTable table : schema.getTables().values()) {
      verifyTable(table);
      final Set<String> columnNames = new HashSet<String>();
      for (final DataSchemaColumn column : table.getColumns()) {
        verifyColumn(column, table.getTableName());
        if (columnNames.contains(column.getName())) {
          error(table, "defines column " + column.getName() + " more than once.");
        }
        columnNames.add(column.getName());
      }
    }
  }

  private static void verifyTable(final DataSchemaTable table) throws VerificationException {
    if (table.getExpirationPhase() != null) {
      if (table.getExpirationPhase() < 0) {
        error(table, "has a negative expiration phase");
      }
      //      if (table.getExpirationPhase() >= InfrastructureConstants.PIPELINE_PHASES) {
      //        error(table, "has an expiration phase greater than "
      //            + (InfrastructureConstants.PIPELINE_PHASES - 1));
      //      }

    }
  }

  private static void verifyColumn(final DataSchemaColumn column, final String tableName)
      throws VerificationException {
    if (column.getName() == null) {
      throw new VerificationException("Column of table " + tableName + " has no name");
    }
    if (column.getType() == null) {
      error(column, tableName, "has no type");
    }
    if (column.getType().equals(DataSchemaType.VarChar)) {
      if (column.getLength() == null) {
        error(column, tableName, "is of type varchar but has no length");
      }
      if (column.getLength() == 0) {
        error(column, tableName, "is of type varchar but has zero length");
      }
    }
  }

  private static void error(final DataSchemaTable table, final String msg)
      throws VerificationException {
    throw new VerificationException("Table " + table.getTableName() + " " + msg);
  }

  private static void error(final DataSchemaColumn column, final String tableName, final String msg)
      throws VerificationException {
    throw new VerificationException(
        "Column " + column.getName() + " of table " + tableName + " " + msg);
  }

}
