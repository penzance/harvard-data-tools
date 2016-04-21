package edu.harvard.data.identity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

// Create Redshift table using:
// CREATE TABLE identity_map (
//     research_id VARCHAR(255),
//     huid VARCHAR(255),
//     xid VARCHAR(255),
//     canvas_id BIGINT,
//     canvas_data_id BIGINT
// );

public class IdentityMap implements DataTable, Comparable<IdentityMap> {

  private static final String ID_TABLE_NAME = "identity_map";

  private final Map<IdentifierType, Object> identities;

  public IdentityMap() {
    this.identities = new HashMap<IdentifierType, Object>();
  }

  public IdentityMap(final CSVRecord record) {
    this();
    this.populate(record);
  }

  public IdentityMap(final TableFormat format, final CSVRecord record) {
    this(record);
  }

  public IdentityMap(final ResultSet resultSet) throws SQLException {
    this();
    this.populate(resultSet);
  }

  public void populate(final CSVRecord record) {
    if (record.get(0) != null) {
      identities.put(IdentifierType.ResearchUUID, record.get(0));
    }
    if (record.get(1) != null) {
      identities.put(IdentifierType.HUID, record.get(1));
    }
    if (record.get(2) != null) {
      identities.put(IdentifierType.XID, record.get(2));
    }
    final String $canvasId = record.get(3);
    if ($canvasId != null && $canvasId.length() > 0) {
      identities.put(IdentifierType.CanvasID, Long.valueOf($canvasId));
    }
    final String $canvasDataId = record.get(4);
    if ($canvasDataId != null && $canvasDataId.length() > 0) {
      identities.put(IdentifierType.CanvasDataID, Long.valueOf($canvasDataId));
    }
  }

  public void populate(final ResultSet resultSet) throws SQLException {
    if (resultSet.getString("research_id") != null) {
      identities.put(IdentifierType.ResearchUUID, "research_id");
    }
    if (resultSet.getString("huid") != null) {
      identities.put(IdentifierType.HUID, "huid");
    }
    if (resultSet.getString("xid") != null) {
      identities.put(IdentifierType.XID, "xid");
    }
    final long canvasId = resultSet.getLong("canvas_id");
    if (!resultSet.wasNull()) {
      identities.put(IdentifierType.CanvasID, canvasId);
    }
    final long canvasDataId = resultSet.getLong("canvas_data_id");
    if (!resultSet.wasNull()) {
      identities.put(IdentifierType.CanvasDataID, canvasDataId);
    }
  }

  @Override
  public List<Object> getFieldsAsList(final TableFormat formatter) {
    final List<Object> fields = new ArrayList<Object>();
    fields.add(identities.get(IdentifierType.ResearchUUID));
    fields.add(identities.get(IdentifierType.HUID));
    fields.add(identities.get(IdentifierType.XID));
    fields.add(identities.get(IdentifierType.CanvasID));
    fields.add(identities.get(IdentifierType.CanvasDataID));
    return fields;
  }

  @Override
  public List<String> getFieldNames() {
    final List<String> fields = new ArrayList<String>();
    fields.add(IdentifierType.ResearchUUID.getFieldName());
    fields.add(IdentifierType.HUID.getFieldName());
    fields.add(IdentifierType.XID.getFieldName());
    fields.add(IdentifierType.CanvasID.getFieldName());
    fields.add(IdentifierType.CanvasDataID.getFieldName());
    return fields;
  }

  @Override
  public String toString() {
    String s = "";
    for (final IdentifierType type : IdentifierType.values()) {
      if (type != IdentifierType.Other) {
        s += type.getFieldName() + ": " + identities.get(type) + " ";
      }
    }
    return s.trim();
  }

  @Override
  public int compareTo(final IdentityMap o) {
    return toString().compareTo(o.toString());
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || !(o instanceof IdentityMap)) {
      return false;
    }
    final IdentityMap other = (IdentityMap)o;
    for (final IdentifierType type : IdentifierType.values()) {
      final Object v1 = identities.get(type);
      final Object v2 = other.identities.get(type);
      if (v1 == null) {
        if (v2 != null) {
          return false;
        }
      } else {
        if (v2 == null) {
          return false;
        }
        if (!v1.equals(v2)) {
          return false;
        }
      }
    }
    return true;
  }

  public PreparedStatement getLookupSqlQuery(final Connection connection) throws SQLException {
    final List<String> params = new ArrayList<String>();
    final List<Object> vals = new ArrayList<Object>();
    final String query = getLookupSql(params, vals);
    final PreparedStatement statement = connection.prepareStatement(query);
    for (int i = 0; i < params.size(); i++) {
      if (vals.get(i) instanceof Long) {
        statement.setLong(i, (Long) vals.get(i));
      } else if (vals.get(i) instanceof String) {
        statement.setString(i, (String) vals.get(i));
      } else {
        throw new RuntimeException("Unknown identity type for " + params.get(i));
      }
    }
    return statement;
  }

  private String getLookupSql(final List<String> params, final List<Object> vals) {
    for (final IdentifierType type : IdentifierType.values()) {
      if (type != IdentifierType.Other) {
        if (identities.containsKey(type)) {
          params.add(type.getFieldName() + " = ?");
          vals.add(identities.get(type));
        }
      }
    }

    String query = "SELECT research_id, huid, xid, canvas_id, canvas_data_id FROM " + ID_TABLE_NAME
        + " WHERE ";
    for (int i = 0; i < params.size(); i++) {
      if (i > 0) {
        query += " OR ";
      }
      query += params.get(i);
    }
    query += ";";
    return query;
  }

  @Override
  public Map<String, Object> getFieldsAsMap() {
    final Map<String, Object> fields = new HashMap<String, Object>();
    if (identities.containsKey(IdentifierType.ResearchUUID)) {
      fields.put("research_id", identities.get(IdentifierType.ResearchUUID));
    }
    if (identities.containsKey(IdentifierType.HUID)) {
      fields.put("huid", identities.get(IdentifierType.HUID));
    }
    if (identities.containsKey(IdentifierType.XID)) {
      fields.put("xid", identities.get(IdentifierType.XID));
    }
    if (identities.containsKey(IdentifierType.CanvasID)) {
      fields.put("canvas_id", identities.get(IdentifierType.CanvasID));
    }
    if (identities.containsKey(IdentifierType.CanvasDataID)) {
      fields.put("canvas_data_id", identities.get(IdentifierType.CanvasDataID));
    }
    return fields;
  }

  public Object get(final IdentifierType idType) {
    return identities.get(idType);
  }

  public void set(final IdentifierType idType, final Object value) {
    identities.put(idType, value);
  }
}
