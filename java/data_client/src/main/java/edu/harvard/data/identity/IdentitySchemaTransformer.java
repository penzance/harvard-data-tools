package edu.harvard.data.identity;

import java.util.List;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;
import edu.harvard.data.schema.identity.IdentitySchema;

/**
 * This class modifies a {@link DataSchema} according to a specification of
 * identifying fields in the various tables of the data set. The identifying
 * tables in the resulting schema have the identifying fields removed and
 * replaced by {@code String} typed Research UUID fields that can store the
 * research ID values generated for an individual.
 * <p>
 * The identifiers in a data set are passed to this class via an
 * {@link IdentitySchema} object, but are typically specified using a JSON
 * resource. See the documentation for {@link IdentitySchema} for details of the
 * JSON format.
 */
public class IdentitySchemaTransformer {

  public static final String RESEARCH_UUID_SUFFIX = "_research_uuid";

  private final DataSchema base;
  private final IdentitySchema identifiers;
  private final IdentifierType mainIdentifier;
  private DataSchema schema;

  public IdentitySchemaTransformer(final DataSchema base, final IdentitySchema identifiers,
      final IdentifierType mainIdentifier) {
    this.base = base;
    this.identifiers = identifiers;
    this.mainIdentifier = mainIdentifier;
  }

  /**
   * Transform the original schema according to the specification above. This
   * method is idempotent; calling it multiple times will return the same
   * {@link DataSchema} object.
   *
   * This method clones the base schema, so that the transformations performed
   * do not affect the structure of the base schema.
   *
   * @return a transformed version of the base schema passed to the constructor.
   * @throws VerificationException
   *           if an error occurs while applying the schema transformations.
   */
  public DataSchema transform() throws VerificationException {
    if (schema == null) {
      final DataSchema schemaCopy = base.copy();
      for (final String tableName : identifiers.tableNames()) {
        final DataSchemaTable table = schemaCopy.getTableByName(tableName);
        if (table == null) {
          throw new VerificationException(
              "Identifier specified for missing table '" + tableName + "'");
        }
        final List<DataSchemaColumn> columns = table.getColumns();
        for (final String columnName : identifiers.get(tableName).keySet()) {
          if (table.getColumn(columnName) == null) {
            throw new VerificationException("Identifier specified for missing column '" + columnName
                + "' of table '" + tableName + "'");
          }
          final DataSchemaColumn oldColumn = table.getColumn(columnName);
          columns.remove(oldColumn);
          if (identifiers.get(tableName).get(columnName).contains(mainIdentifier)) {
            final String ridColumn = columnName + RESEARCH_UUID_SUFFIX;
            final ExtensionSchemaColumn newColumn = new ExtensionSchemaColumn(ridColumn,
                oldColumn.getDescription()
                + ". Value replaced by a UUID generated for the research data set.",
                "varchar", 255);
            newColumn.setNewlyGenerated(true);
            columns.add(newColumn);
          }
        }
      }
      schema = schemaCopy;
    }
    return schema;
  }
}
