package edu.harvard.data.identity;

import java.util.List;
import java.util.Map;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;

public class IdentitySchemaTransformer {

  public static final String RESEARCH_UUID_SUFFIX = "_research_uuid";

  public DataSchema transform(final DataSchema base,
      final Map<String, Map<String, List<IdentifierType>>> identifiers,
      final IdentifierType mainIdentifier) throws VerificationException {
    final DataSchema schema = base.copy();
    for (final String tableName : identifiers.keySet()) {
      final DataSchemaTable table = schema.getTableByName(tableName);
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
    return schema;
  }
}
