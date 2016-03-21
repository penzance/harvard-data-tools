package edu.harvard.data.identity;

import java.util.List;
import java.util.Map;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;

public class IdentitySchemaTransformer {

  public DataSchema transform(final DataSchema base,
      final Map<String, Map<String, List<IdentifierType>>> identifiers)
          throws VerificationException {
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
        columns.remove(table.getColumn(columnName));
      }
      final ExtensionSchemaColumn newColumn = new ExtensionSchemaColumn("research_uuid",
          "UUID generated for research data set.", "varchar", 255);
      newColumn.setNewlyGenerated(true);
      columns.add(newColumn);
    }
    return schema;
  }
}
