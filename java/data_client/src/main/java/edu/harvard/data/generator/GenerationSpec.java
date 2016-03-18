package edu.harvard.data.generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

public class GenerationSpec {
  private static final Logger log = LogManager.getLogger();

  private final List<SchemaPhase> phases;
  private final ObjectMapper jsonMapper;
  private File outputBase;
  private String tableEnumName;

  public GenerationSpec(final int phaseCount) {
    this.phases = new ArrayList<SchemaPhase>();
    for (int i = 0; i < phaseCount; i++) {
      phases.add(new SchemaPhase());
    }
    this.jsonMapper = new ObjectMapper();
    this.jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
  }

  public SchemaPhase getPhase(final int i) {
    return phases.get(i);
  }

  public File getOutputBase() {
    return outputBase;
  }

  public String getJavaTableEnumName() {
    return tableEnumName;
  }

  public SchemaPhase getLastPhase() {
    return phases.get(phases.size() - 1);
  }

  public void setOutputBaseDirectory(final File outputBase) {
    this.outputBase = outputBase;
  }

  public void setJavaPackages(final String... packageList) {
    if (packageList.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " packages, got " + packageList.length);
    }
    for (int i = 0; i < packageList.length; i++) {
      phases.get(i).setJavaPackage(packageList[i]);
    }
  }

  public void setPrefixes(final String... prefixes) {
    if (prefixes.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " class prefixes, got " + prefixes.length);
    }
    for (int i = 0; i < prefixes.length; i++) {
      phases.get(i).setPrefix(prefixes[i]);
    }
  }

  public void setJavaTableEnumName(final String tableEnumName) {
    this.tableEnumName = tableEnumName;
  }

  public void setHdfsDirectories(final String... hdfsDirs) {
    if (hdfsDirs.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " HDFS directories, got " + hdfsDirs.length);
    }
    for (int i = 0; i < hdfsDirs.length; i++) {
      phases.get(i).setHDFSDir(hdfsDirs[i]);
    }
  }

  public void setSchemas(final DataSchema schema, final String... transformationResources)
      throws IOException, VerificationException {
    if (transformationResources.length != phases.size() - 1) {
      throw new RuntimeException("Expected " + (phases.size() - 1)
          + " transformation resources, got " + transformationResources.length);
    }

    // we're going to change the table structure; make a copy first.
    final DataSchema schemaCopy = schema.copy();

    phases.get(0).setSchema(schemaCopy.copy());
    for (int i = 1; i < phases.size(); i++) {
      setNewGeneratedFlags(schemaCopy.getTables().values(), false);
      for (final DataSchemaTable table : schemaCopy.getTables().values()) {
        table.setOwner(null);
      }
      extendTableSchema(schemaCopy, transformationResources[i - 1]);
      phases.get(i).setSchema(schemaCopy.copy());
    }
  }

  // Read the JSON file and add any new tables or fields to the schema. If a
  // table in the JSON file does not exist in the schema, it is created. If a
  // table does exist, any fields specified in the JSON are appended to the
  // field list for that table.
  private void extendTableSchema(final DataSchema schema, final String jsonResource)
      throws IOException, VerificationException {
    log.info("Extending schema from file " + jsonResource);
    final ClassLoader classLoader = this.getClass().getClassLoader();
    Map<String, DataSchemaTable> updates;
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      updates = jsonMapper.readValue(in, ExtensionSchema.class).getTables();
    }
    if (updates != null) {
      // Track tables and columns that were added in this update
      setNewGeneratedFlags(updates.values(), true);
      for (final String tableName : updates.keySet()) {
        final ExtensionSchemaTable newTable = (ExtensionSchemaTable) updates.get(tableName);
        newTable.setTableName(tableName);
        handleLikeField(newTable, updates, schema);
        if (!schema.getTables().containsKey(tableName)) {
          schema.getTables().put(tableName, newTable);
        } else {
          final DataSchemaTable originalTable = schema.getTables().get(tableName);
          for (final DataSchemaColumn column : newTable.getColumns()) {
            if (originalTable.getColumn(column.getName()) != null) {
              throw new VerificationException("Redefining " + column.getName() + " in table " + tableName);
            }
            originalTable.getColumns().add(column);
          }
          originalTable.setOwner(newTable.getOwner());
        }
      }
    }
  }

  private void handleLikeField(final DataSchemaTable table,
      final Map<String, DataSchemaTable> updates, final DataSchema schema)
          throws VerificationException {
    final String likeTableName = table.getLikeTable();
    final Map<String, DataSchemaType> seenColumns = new HashMap<String, DataSchemaType>();
    for (final DataSchemaColumn column : table.getColumns()) {
      seenColumns.put(column.getName(), column.getType());
    }
    if (likeTableName != null) {
      final String tableName = table.getTableName();
      if (!updates.containsKey(tableName) && schema.getTableByName(tableName) == null) {
        throw new VerificationException(
            "Table " + tableName + " specified to be like missing table " + likeTableName);
      }
      addColumns(table, updates.get(likeTableName), seenColumns);
      addColumns(table, schema.getTableByName(likeTableName), seenColumns);
    }
  }

  private void addColumns(final DataSchemaTable table, final DataSchemaTable likeTable,
      final Map<String, DataSchemaType> seenColumns) throws VerificationException {
    if (likeTable != null) {
      for (final DataSchemaColumn column : likeTable.getColumns()) {
        final String columnName = column.getName();
        final DataSchemaType columnType = column.getType();
        if (seenColumns.containsKey(columnName)) {
          if (!seenColumns.get(columnName).equals(columnType)) {
            throw new VerificationException(
                "Redefining " + columnName + " of table " + table.getTableName() + " from "
                    + seenColumns.get(columnName) + " to " + columnType);
          }
        } else {
          table.getColumns().add(0, column);
        }
        seenColumns.put(columnName, columnType);
      }
    }
  }

  // Bulk-set the newGenerated flags on a set of tables.
  private void setNewGeneratedFlags(final Collection<DataSchemaTable> tableSet,
      final boolean flag) {
    for (final DataSchemaTable table : tableSet) {
      table.setNewlyGenerated(flag);
      for (final DataSchemaColumn column : table.getColumns()) {
        column.setNewlyGenerated(flag);
      }
    }
  }
}
