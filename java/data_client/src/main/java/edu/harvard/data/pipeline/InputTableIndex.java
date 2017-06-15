package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.generator.SchemaPhase;
import edu.harvard.data.schema.DataSchemaTable;

/**
 * This class provides a detailed listing of all data files stored on S3 for a
 * given data dump. It is produced as part of the Phase0 processing step, and is
 * used both by the code generator to determine which files and statements to
 * generate, and the pipeline setup to select which processing phases are
 * required for a given data pipeline run.
 */
public class InputTableIndex {
  private String schemaVersion;
  private final Map<String, List<String>> tables;
  private final Map<String, Long> fileSizes;
  private final Map<String, Boolean> partial;

  public InputTableIndex() {
    this.tables = new HashMap<String, List<String>>();
    this.partial = new HashMap<String, Boolean>();
    this.fileSizes = new HashMap<String, Long>();
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(final String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  /**
   * Add a new S3-based file to the index, updating the various internal tables
   * to keep track of file sizes and table mappings.
   *
   * @param table
   *          the name of the data table to which this file belongs. The file
   *          must be formatted in a manner that can be parsed into an instance
   *          of that data table.
   * @param file
   *          a reference to the file stored on S3.
   * @param sizeBytes
   *          the size of the file object, in bytes.
   */
  public void addFile(final String table, final S3ObjectId file, final long sizeBytes) {
    if (!tables.containsKey(table)) {
      tables.put(table, new ArrayList<String>());
    }
    tables.get(table).add(AwsUtils.uri(file));
    fileSizes.put(AwsUtils.uri(file), sizeBytes);
  }

  /**
   * Get the full set of tables that have been stored in the index.
   *
   * @return a map of all files, indexed by table name.
   */
  public Map<String, List<String>> getTables() {
    return tables;
  }

  /**
   * Get a list of table names only, with no file information.
   *
   * @return a list of table names.
   */
  public List<String> getTableNames() {
    final List<String> names = new ArrayList<String>(tables.keySet());
    Collections.sort(names);
    return names;
  }

  /**
   * Get all the files that are associated with a given table.
   *
   * @param table
   *          the name of the table to get all files for.
   *
   * @return a list of references to files stored on S3.
   */
  public List<S3ObjectId> getFiles(final String table) {
    final List<S3ObjectId> files = new ArrayList<S3ObjectId>();
    for (final String file : tables.get(table)) {
      files.add(AwsUtils.key(file));
    }
    return files;
  }

  /**
   * Merge the data stored in another InputTableIndex into this one. Should both
   * indices contain the same table name, the set of files associated with that
   * table will be the union of both file sets.
   *
   * @param data
   *          another InputTableIndex object with which to merge. The index
   *          represented by the parameter will not be modified.
   */
  public void addAll(final InputTableIndex data) {
    fileSizes.putAll(data.fileSizes);
    for (final String table : data.tables.keySet()) {
      if (!tables.containsKey(table)) {
        tables.put(table, new ArrayList<String>());
      }
      tables.get(table).addAll(data.tables.get(table));
    }
  }

  /**
   * Select a single table from another InputTableIndex, and add all related
   * files to the file set for that table in this index. If the table does not
   * exist in this index, an entry will be created for it.
   *
   * @param table
   *          the name of the table to add to the index. The InputTableIndex
   *          passed to the method must include a table by this name.
   * @param data
   *          another InputTableIndex object from which to add the table's data.
   *          The index represented by the parameter will not be modified.
   */
  public void addTable(final String table, final InputTableIndex data) {
    if (!tables.containsKey(table)) {
      tables.put(table, new ArrayList<String>());
    }
    tables.get(table).addAll(data.tables.get(table));
    for (final String file : data.tables.get(table)) {
      fileSizes.put(file, data.fileSizes.get(file));
    }
  }

  /**
   * Get the sizes of all files in the index.
   *
   * @return a map from the S3 file identifier (formatted as a standard s3://
   *         URL) to the file size (in bytes).
   */
  public Map<String, Long> getFileSizes() {
    return fileSizes;
  }

  /**
   * Get the file size for a specific file stored on S3.
   *
   * @param file
   *          a reference to the file stored on S3.
   *
   * @return the size of the file, in bytes.
   */
  public Long getFileSize(final S3ObjectId file) {
    return fileSizes.get(AwsUtils.uri(file));
  }

  @Override
  public String toString() {
    String s = "Schema: " + schemaVersion + "\n";
    for (final String key : getTableNames()) {
      s += key + ":\n";
      for (final S3ObjectId file : getFiles(key)) {
        s += "  " + AwsUtils.uri(file) + "\n";
      }
    }
    return s;
  }

  public boolean containsTable(final String table) {
    return tables.containsKey(table);
  }

  public static InputTableIndex read(final AwsUtils aws, final S3ObjectId location)
      throws IOException {
    return aws.readJson(location, InputTableIndex.class);
  }

  public void setPartial(final String table, final boolean isPartial) {
    partial.put(table, isPartial);
  }

  public void setPartial(final Map<String, Boolean> data) {
    this.partial.putAll(data);
  }

  public Map<String, Boolean> getPartial() {
    return partial;
  }

  public boolean isPartial(final String tableName) {
    return partial.get(tableName);
  }

  public boolean canvasMegadump() {
    return containsTable("requests") && !isPartial("requests");
  }

  public void addNewlyGeneratedTables(final List<SchemaPhase> schemaPhases) {
    for (final SchemaPhase phase : schemaPhases) {
      for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
        if (table.getNewlyGenerated()) {
          this.tables.put(table.getTableName(), new ArrayList<String>());
          this.partial.put(table.getTableName(), false); // XXX Need to figure
          // this out per-table
          this.fileSizes.put(table.getTableName(), 0L);
        }
      }
    }
  }
}
