package edu.harvard.data.generator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

/**
 * Utility class that extends a data schema with new tables and fields in order
 * to generate SDKs for different phases of the data processing pipeline.
 */
public class SchemaTransformer {

  /**
   * Create a new schema that combines a base schema with a set of new tables
   * and columns. The resulting schema will have the 'newlyGenerated' flag set
   * on all tables and columns from the extension schema, and unset on all
   * tables and columns from the base schema.
   *
   * @param base
   *          a {@link DataSchema} that is to be extended.
   * @param extension
   *          an {@link ExtensionSchema} containing new tables and columns to be
   *          added.
   * @return a new {@code DataSchema} containing all tables and columns from
   *         both the base and extension schemas. This object is constructed
   *         from a deep copy of the base and extension schemas, so
   *         modifications will not affect the originals.
   * @throws VerificationException
   *           if the extension schema contains duplicate fields, or is
   *           specified to be 'like' a non-existent table.
   */
  public DataSchema transform(final DataSchema base, final ExtensionSchema extension)
      throws VerificationException {
    // The new schema we're going to return will have all of the columns of the
    // original. We make a copy of the base schema so that we can edit it
    // safely.
    final DataSchema schema = base.copy();

    // All tables and columns in the new schema were copied from the base
    // schema, and so are not newly-generated.
    setNewGeneratedFlags(schema.getTables().values(), false);

    // If a table is owned it means that some process is going to write to it in
    // a given phase (see the TableOwner enum for possible owners). Since the
    // base schema came from a different phase, its tables' ownership is not
    // relevant for the new schema.
    for (final DataSchemaTable table : schema.getTables().values()) {
      table.setOwner(null);
    }

    // Add any new tables and columns from the extension schema.
    extendTableSchema(schema, (ExtensionSchema) extension.copy());

    return schema;
  }

  /**
   * Add any new tables or fields to the schema. If a table in the extension
   * schema does not exist in the new schema, it is created. If a table does
   * exist, any fields specified in the extension schema are appended to the
   * field list for that table.
   */
  private void extendTableSchema(final DataSchema schema, final ExtensionSchema extension)
      throws VerificationException {
    final Map<String, DataSchemaTable> updates = extension.getTables();
    if (updates != null) {
      // Track tables and columns that were added in this update
      setNewGeneratedFlags(updates.values(), true);

      for (final String tableName : updates.keySet()) {
        final ExtensionSchemaTable newTable = (ExtensionSchemaTable) updates.get(tableName);
        // The schema extension format does not require a tableName field; that
        // way we can ensure that the table name matches the key in the
        // extension schema tables map.
        newTable.setTableName(tableName);

        // The extension schema format allows the user to specify 'like' for a
        // table, which copies all the columns from an existing table.
        handleLikeField(newTable, updates, schema);

        if (!schema.getTables().containsKey(tableName)) {
          // The new table doesn't exist yet - copy it into the schema.
          schema.getTables().put(tableName, newTable);
        } else {
          // The new table already exists in the schema. Copy its columns.
          final DataSchemaTable originalTable = schema.getTables().get(tableName);
          for (final DataSchemaColumn column : newTable.getColumns()) {
            if (originalTable.getColumn(column.getName()) != null) {
              // The new column already exists in the table (could happen as a
              // result of using the 'like' construct).
              throw new VerificationException(
                  "Redefining " + column.getName() + " in table " + tableName);
            }
            originalTable.getColumns().add(column);
          }
          // If the table is set as owned in the extension schema, we need to
          // reflect that in the new schema.
          originalTable.setOwner(newTable.getOwner());
        }
      }
    }
  }

  /**
   * If one table is set to be 'like' another, we copy all the columns from the
   * 'like table' in to the new table.
   */
  private void handleLikeField(final DataSchemaTable table,
      final Map<String, DataSchemaTable> updates, final DataSchema schema)
          throws VerificationException {
    final String likeTableName = table.getLikeTable();
    if (likeTableName != null) {
      // Keep track of columns that we have already copied. This is important if
      // the like table is also extended in the extension schema. In that case,
      // depending on the order in which the tables are processed both the new
      // schema and the extension schema may define the same fields. Keeping a
      // map eliminates any duplicates, and ensures that a user doesn't
      // accidentally define the same field multiple times with different types.
      final Map<String, DataSchemaType> seenColumns = new HashMap<String, DataSchemaType>();
      for (final DataSchemaColumn column : table.getColumns()) {
        seenColumns.put(column.getName(), column.getType());
      }

      // Check that the original table exists.
      final String tableName = table.getTableName();
      if (!updates.containsKey(tableName) && schema.getTableByName(tableName) == null) {
        throw new VerificationException(
            "Table " + tableName + " specified to be like missing table " + likeTableName);
      }
      // Add any columns defined in the extension schema. We'll be prepending
      // columns as they are copied, so by copying extension columns first we
      // ensure that new columns appear later in the schema than those from the
      // original table.
      addColumns(table, updates.get(likeTableName), seenColumns);
      // Add all columns from the original table.
      addColumns(table, schema.getTableByName(likeTableName), seenColumns);
    }
  }

  /**
   * Copy all columns from one schema to another. If a column has already been
   * copied (we can tell because it's in the seenColumns map), we skip it. As an
   * extra check, we ensure that any duplicate columns are of the same type.
   */
  private void addColumns(final DataSchemaTable table, final DataSchemaTable likeTable,
      final Map<String, DataSchemaType> seenColumns) throws VerificationException {
    if (likeTable != null) {
      for (final DataSchemaColumn column : likeTable.getColumns()) {
        final String columnName = column.getName();
        final DataSchemaType columnType = column.getType();

        // Check for duplicate columns, and throw an error if they're of
        // different types.
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

  /**
   * Bulk-set the newGenerated flags on a set of tables.
   */
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
