package edu.harvard.data.client.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.harvard.data.client.generator.schema.ExtensionSchemaTable;

public class SchemaComparison {
  private final DataSchema originalSchema;
  private final DataSchema newSchema;
  private final Map<String, DataSchemaTable> additions;
  private final Map<String, DataSchemaTable> deletions;

  public SchemaComparison(final DataSchema originalSchema, final DataSchema newSchema) {
    this.originalSchema = originalSchema.copy();
    this.newSchema = newSchema.copy();
    this.additions = getAdditions(originalSchema, newSchema);
    this.deletions = getAdditions(newSchema, originalSchema);
  }

  private Map<String, DataSchemaTable> getAdditions(final DataSchema s1, final DataSchema s2) {
    final Map<String, DataSchemaTable> changes = new HashMap<String, DataSchemaTable>();
    final List<String> tableNames = getTableNames(s1.getTables());
    for (final String tableName : tableNames) {
      final DataSchemaTable table = s1.getTableByName(tableName);
      if (s2.getTableByName(tableName) != null) {
        final DataSchemaTable tableDifferences = getTableAdditions(table,
            s2.getTableByName(tableName));
        if (!tableDifferences.getColumns().isEmpty()) {
          changes.put(tableName, tableDifferences);
        }
      } else {
        final DataSchemaTable tableCopy = table.copy();
        tableCopy.setNewlyGenerated(true);
        changes.put(tableName, tableCopy);
      }
    }
    return changes;
  }

  private DataSchemaTable getTableAdditions(final DataSchemaTable t1, final DataSchemaTable t2) {
    final List<DataSchemaColumn> changes = new ArrayList<DataSchemaColumn>();
    for (int i = 0; i < t1.getColumns().size(); i++) {
      if (!(i < t2.getColumns().size()
          && t1.getColumns().get(i).getName().equals(t2.getColumns().get(i).getName()))) {
        final DataSchemaColumn column = t1.getColumns().get(i).copy();
        column.setNewlyGenerated(true);
        changes.add(column);
      }
    }
    return new ExtensionSchemaTable(t1.getTableName(), changes);
  }

  private List<String> getTableNames(final Map<String, DataSchemaTable> tables) {
    final List<String> names = new ArrayList<String>();
    for (final DataSchemaTable t : tables.values()) {
      names.add(t.getTableName());
    }
    Collections.sort(names);
    return names;
  }

  public Map<String, DataSchemaTable> getAdditions() {
    return additions;
  }

  public Map<String, DataSchemaTable> getDeletions() {
    return deletions;
  }

  public DataSchema getOriginalSchema() {
    return originalSchema;
  }

  public DataSchema getNewSchema() {
    return newSchema;
  }

  @Override
  public String toString() {
    String s = "";
    if (!additions.isEmpty()) {
      s += "\nAdditions\n=========";
      for (final String n : additions.keySet()) {
        s += "\n" + additions.get(n);
      }
    }
    if (!deletions.isEmpty()) {
      s += "\nDeletions\n=========";
      for (final String n : deletions.keySet()) {
        s += "\n" + deletions.get(n);
      }
    }
    return s;
  }

}
