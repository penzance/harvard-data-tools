package edu.harvard.data.schema.redshift;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;

public class RedshiftSchema implements DataSchema {

  private final Map<String, DataSchemaTable> tables;

  public RedshiftSchema(final ResultSet rs) throws SQLException {
    final HashMap<String, Map<Integer, DataSchemaColumn>> tmpMap = new HashMap<String, Map<Integer, DataSchemaColumn>>();
    while (rs.next()) {
      final String tableName = rs.getString("table_name");
      final RedshiftColumn column = new RedshiftColumn(rs);
      if (!tmpMap.containsKey(tableName)) {
        tmpMap.put(tableName, new HashMap<Integer, DataSchemaColumn>());
      }
      tmpMap.get(tableName).put(rs.getInt("ordinal_position"), column);
    }

    tables = new HashMap<String, DataSchemaTable>();
    for (final String tableName : tmpMap.keySet()) {
      final Map<Integer, DataSchemaColumn> table = tmpMap.get(tableName);
      tables.put(tableName, new RedshiftTable(tableName, table));
    }
  }

  @Override
  public Map<String, DataSchemaTable> getTables() {
    return tables;
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public DataSchema copy() {
    return null;
  }

  @Override
  public String toString() {
    String s = "";
    final List<String> names = new ArrayList<String>(tables.keySet());
    Collections.sort(names);
    for (final String name : names) {
      s += tables.get(name) + "\n";
    }
    return s.trim();
  }

  @Override
  public DataSchemaTable getTableByName(final String name) {
    return tables.get(name);
  }
}
