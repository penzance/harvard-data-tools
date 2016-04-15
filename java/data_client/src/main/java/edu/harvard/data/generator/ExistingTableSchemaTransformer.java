package edu.harvard.data.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.existing.ExistingSchema;
import edu.harvard.data.schema.existing.ExistingSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

public class ExistingTableSchemaTransformer {

  /**
   * Create a new {@link DataSchema} object that contains all the tables of the
   * base schema, plus any additional tables specified by an
   * {@link ExistingSchema} definition.
   *
   * @param base
   *          the initial schema on which the new schema will be based. All
   *          tables from this schema will be included in the resulting schema.
   * @param existing
   *          an {@code ExistingSchema} object that describes any additional
   *          tables to be added to the base schema.
   * @param extensions
   *          the fully-transformed set of tables that represent the current
   *          state of the database. Since an existing table is based on a table
   *          elsewhere in the schema, this method requires any table
   *          definitions that may be referenced in the existing table
   *          specification, regardless of when they are defined.
   *
   * @return a new {@code DataSchema} object that contains a copy of all tables
   *         in the base schema, plus any new tables defined in the
   *         {@code ExistingSchema} specification.
   *
   * @throws VerificationException
   *           if the {@code ExistingSchema} specification does not match the
   *           other schemas presented to this method.
   */
  public DataSchema transform(final DataSchema base, final ExistingSchema existing,
      final DataSchema finalSchema) throws VerificationException {
    // We'll be modifying the schema, so we make a copy for safety.
    final DataSchema schema = base.copy();
    final Map<String, DataSchemaTable> tables = copyExistingTables(base, finalSchema);

    for (final String tableName : existing.getTables().keySet()) {
      final ExistingSchemaTable existingTable = existing.getTables().get(tableName);
      final DataSchemaTable srcTable = getSourceTable(existingTable, tables);
      final DataSchemaTable newTable = buildTable(srcTable, existingTable);
      schema.addTable(newTable.getTableName(), newTable);
    }

    return schema;
  }

  // Create a new DataSchemaTable object containing the columns of the srcTable
  // with the exception of any columns named in the exclude set.
  private DataSchemaTable buildTable(final DataSchemaTable srcTable,
      final ExistingSchemaTable existingTable) throws VerificationException {
    // Verify that all excluded tables actually exist in the source table.
    for (final String exclusion : existingTable.getExclude()) {
      final DataSchemaColumn excludedColumn = srcTable.getColumn(exclusion);
      if (excludedColumn == null) {
        throw new VerificationException("Existing table " + existingTable.getTableName()
        + " excludes missing column " + exclusion);
      }
    }
    if (existingTable.getExpireAfterPhase() == null) {
      throw new VerificationException("Existing table " + existingTable.getTableName()
      + " does not specify an expire_after_phase value");
    }
    if (existingTable.getDays() != null && existingTable.getTimestampColumn() == null) {
      throw new VerificationException("Existing table " + existingTable.getTableName()
      + " requires sets a date range but does not specify timestamp column.");
    }
    if (existingTable.getTimestampColumn() != null) {
      if (srcTable.getColumn(existingTable.getTimestampColumn()) == null) {
        throw new VerificationException("Existing table " + existingTable.getTableName()
        + " requires missing timestamp column '" + existingTable.getTimestampColumn()
        + "' from table " + srcTable.getTableName());
      }
    }
    // Build a list of columns for the new table
    final List<ExtensionSchemaColumn> columns = new ArrayList<ExtensionSchemaColumn>();
    for (final DataSchemaColumn column : srcTable.getColumns()) {
      if (!existingTable.getExclude().contains(column.getName())) {
        columns.add(new ExtensionSchemaColumn(column.getName(), column.getDescription(),
            column.getType().toString(), column.getLength()));
      }
    }
    // Create a new table using the properties of the source
    final ExtensionSchemaTable newTable = new ExtensionSchemaTable(null,
        existingTable.getDescription(), columns, null, existingTable.getExpireAfterPhase());
    newTable.setTableName(existingTable.getTableName());
    return newTable;
  }

  // Every existing table must be based on a table defined somewhere else in the
  // schema. Find that table from the existing table's source_table attribute,
  // and throw an exception if it's not found.
  private DataSchemaTable getSourceTable(final ExistingSchemaTable existingTable,
      final Map<String, DataSchemaTable> tables) throws VerificationException {
    final String srcTableName = existingTable.getSourceTable();
    if (srcTableName == null) {
      throw new VerificationException(
          "Existing table " + existingTable.getTableName() + " must specify a source table");
    }
    final DataSchemaTable srcTable = tables.get(srcTableName);
    if (srcTable == null) {
      throw new VerificationException("Existing table " + existingTable.getTableName()
      + " based on missing table " + srcTableName);
    }
    if (srcTable.isTemporary()) {
      throw new VerificationException("Existing table " + existingTable.getTableName()
      + " cannot be based on temporary table " + srcTableName);
    }
    return srcTable;
  }

  // Populate a map with all tables defined elsewhere in the schema and various
  // extensions. We're not going to modify the original schemas, so we don't
  // need to make copies.
  private Map<String, DataSchemaTable> copyExistingTables(final DataSchema base,
      final DataSchema originalSchema) {
    final Map<String, DataSchemaTable> tables = new HashMap<String, DataSchemaTable>();
    for (final DataSchemaTable table : base.getTables().values()) {
      tables.put(table.getTableName(), table);
    }
    for (final DataSchemaTable table : originalSchema.getTables().values()) {
      tables.put(table.getTableName(), table);
    }
    return tables;
  }

}
