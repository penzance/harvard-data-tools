package edu.harvard.data.data_tool_generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.client.FormatLibrary;
import edu.harvard.data.client.canvas.api.CanvasDataSchema;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaColumn;
import edu.harvard.data.client.canvas.api.CanvasDataSchemaTable;

public class SchemaTransformer {
  private static final Logger log = LogManager.getLogger();

  private final List<SchemaPhase> phases;
  private final ObjectMapper jsonMapper;

  public SchemaTransformer(final int phaseCount) {
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

  public void setJavaSourceLocations(final File... sourceDirs) {
    if (sourceDirs.length != phases.size()) {
      throw new RuntimeException(
          "Expected " + phases.size() + " source directories, got " + sourceDirs.length);
    }
    for (int i = 0; i < sourceDirs.length; i++) {
      phases.get(i).setJavaSourceDir(sourceDirs[i]);
    }
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

  public void setSchemas(final CanvasDataSchema schema, final String... transformationResources)
      throws IOException {
    if (transformationResources.length != phases.size() - 1) {
      throw new RuntimeException("Expected " + (phases.size() - 1)
          + " transformation resources, got " + transformationResources.length);
    }

    // we're going to change the table structure; make a copy first.
    final CanvasDataSchema schemaCopy = new CanvasDataSchema(schema);

    phases.get(0).setSchema(new CanvasDataSchema(schemaCopy));
    for (int i = 1; i < phases.size(); i++) {
      setNewGeneratedFlags(schemaCopy.getSchema().values(), false);
      for (final CanvasDataSchemaTable table : schemaCopy.getSchema().values()) {
        table.setOwner(null);
      }
      extendTableSchema(schemaCopy, transformationResources[i - 1]);
      phases.get(i).setSchema(new CanvasDataSchema(schemaCopy));
    }
  }

  // Read the JSON file and add any new tables or fields to the schema. If a
  // table in the JSON file does not exist in the schema, it is created. If a
  // table does exist, any fields specified in the JSON are appended to the
  // field list for that table.
  private void extendTableSchema(final CanvasDataSchema schema, final String jsonResource)
      throws IOException {
    log.info("Extending schema from file " + jsonResource);
    final ClassLoader classLoader = this.getClass().getClassLoader();
    Map<String, CanvasDataSchemaTable> updates;
    try (final InputStream in = classLoader.getResourceAsStream(jsonResource)) {
      updates = jsonMapper.readValue(in, CanvasDataSchema.class).getSchema();
    }
    if (updates != null) {
      // Track tables and columns that were added in this update
      setNewGeneratedFlags(updates.values(), true);
      for (final String tableName : updates.keySet()) {
        final CanvasDataSchemaTable newTable = updates.get(tableName);
        if (!schema.getSchema().containsKey(tableName)) {
          schema.getSchema().put(tableName, newTable);
        } else {
          final CanvasDataSchemaTable originalTable = schema.getSchema().get(tableName);
          originalTable.getColumns().addAll(newTable.getColumns());
          originalTable.setOwner(newTable.getOwner());
        }
      }
    }
  }

  // Bulk-set the newGenerated flags on a set of tables.
  private void setNewGeneratedFlags(final Collection<CanvasDataSchemaTable> tableSet,
      final boolean flag) {
    for (final CanvasDataSchemaTable table : tableSet) {
      table.setNewGenerated(flag);
      for (final CanvasDataSchemaColumn column : table.getColumns()) {
        column.setNewGenerated(flag);
      }
    }
  }
}
