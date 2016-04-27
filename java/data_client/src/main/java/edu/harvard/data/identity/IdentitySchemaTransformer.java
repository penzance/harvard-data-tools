package edu.harvard.data.identity;

import java.util.List;
import java.util.Map;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;

/**
 * This class modifies a {@link DataSchema} according to a specification of
 * identifying fields in the various tables of the data set. The identifying
 * tables in the resulting schema have the identifying fields removed and
 * replaced by {@code String} typed Research UUID fields that can store the
 * research ID values generated for an individual.
 * <p>
 * The identifiers in a data set are passed to this class via a {@code Map}, but
 * are typically specified using a JSON resource with the following structure:
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
public class IdentitySchemaTransformer {

  public static final String RESEARCH_UUID_SUFFIX = "_research_uuid";

  private final DataSchema base;
  private final Map<String, Map<String, List<IdentifierType>>> identifiers;
  private final IdentifierType mainIdentifier;
  private DataSchema schema;

  public IdentitySchemaTransformer(final DataSchema base,
      final Map<String, Map<String, List<IdentifierType>>> identifiers,
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
   */
  public DataSchema transform() throws VerificationException {
    if (schema == null) {
      final DataSchema schemaCopy = base.copy();
      for (final String tableName : identifiers.keySet()) {
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
