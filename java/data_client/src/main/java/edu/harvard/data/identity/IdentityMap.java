package edu.harvard.data.identity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;

// Create Redshift table using:
// CREATE TABLE pii.identity_map (
//     research_id VARCHAR(255),
//     huid VARCHAR(255),
//     xid VARCHAR(255),
//     canvas_id BIGINT,
//     canvas_data_id BIGINT,
//     eppn VARCHAR(255),
//     active_directory_id VARCHAR(255)
// );
//
// CREATE TABLE pii.name (
//     research_id VARCHAR(255),
//     name VARCHAR(255)
//  );
//
// CREATE TABLE pii.email (
//     research_id VARCHAR(255),
//     email VARCHAR(255)
// );

/**
 * This class represents an entry in the environment-scoped
 * <code>identity_map</code> table. It contains the cumulative information that
 * we gather over many runs of many data sets in order to tie together an
 * individual's various identifiers. As such, it is not scoped to any particular
 * data set.
 *
 * The {@code IdentityMap} class implements the {@link DataTable} interface,
 * meaning that it can be read and written using the streams defined in
 * {@link edu.harvard.data.io}. It is used heavily in the identity phase, as the
 * means of transferring identity information between Hadoop steps (see
 * {@link HadoopIdentityKey} for the Hadoop framework-specific wrapper used that
 * allows the IdentityMap to be used as a key). The {@code IdentityMap} is also
 * treated as any other table in a data set, read as part of the input to the
 * identity phase, and written as part of its output.
 */
public class IdentityMap implements DataTable, Comparable<IdentityMap> {

  private final Map<IdentifierType, Object> identities;

  /**
   * Create a new empty identity map.
   */
  public IdentityMap() {
    this.identities = new HashMap<IdentifierType, Object>();
  }

  /**
   * Create an identity map, reading is initial values from a CSV record.
   *
   * @param record
   *          a properly-formatted {@link CSVRecord} that contains String values
   *          for each identifier (any of which may be null, apart from the
   *          research ID).
   */
  public IdentityMap(final CSVRecord record) {
    this();
    populate(record);
  }

  /**
   * Create an identity map, reading is initial values from a CSV record.
   *
   * @param format
   *          this parameter is ignored by this {@code DataTable} implementation.
   * @param record
   *          a properly-formatted {@link CSVRecord} that contains String values
   *          for each identifier (any of which may be null, apart from the
   *          research ID).
   */
  public IdentityMap(final TableFormat format, final CSVRecord record) {
    this();
    populate(record);
  }

  public static Map<String, DataSchemaTable> getIdentityMapTables() {
    final Map<String, DataSchemaTable> tables = new HashMap<String, DataSchemaTable>();
    List<DataSchemaColumn> columns = new ArrayList<DataSchemaColumn>();
    columns.add(new ExtensionSchemaColumn("research_id", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("huid", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("xid", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("canvas_id", "", "bigint", 0));
    columns.add(new ExtensionSchemaColumn("canvas_data_id", "", "bigint", 0));
    columns.add(new ExtensionSchemaColumn("eppn", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("active_directory_id", "", "varchar", 255));
    tables.put("identity_map", new ExtensionSchemaTable("identity_map", columns));

    columns = new ArrayList<DataSchemaColumn>();
    columns.add(new ExtensionSchemaColumn("research_id", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("name", "", "varchar", 255));
    tables.put("name", new ExtensionSchemaTable("name", columns));

    columns = new ArrayList<DataSchemaColumn>();
    columns.add(new ExtensionSchemaColumn("research_id", "", "varchar", 255));
    columns.add(new ExtensionSchemaColumn("email", "", "varchar", 255));
    tables.put("email", new ExtensionSchemaTable("email", columns));

    return tables;
  }

  public static List<String> getPrimaryKeyFields(final String tableName) {
    final List<String> keys = new ArrayList<String>();
    switch(tableName) {
    case "identity_map":
      keys.add("research_id");
      break;
    case "name":
      keys.add("research_id");
      keys.add("name");
      break;
    case "email":
      keys.add("research_id");
      keys.add("email");
      break;
    default:
      throw new RuntimeException("Unknown identity table " + tableName);
    }
    return keys;
  }

  private void populate(final CSVRecord record) {
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
    if (record.get(5) != null) {
      identities.put(IdentifierType.EPPN, record.get(5));
    }
    if (record.get(6) != null) {
      identities.put(IdentifierType.ActiveDirectoryID, record.get(6));
    }
  }

  /**
   * Create an identity map, using initial values that were obtained through
   * JDBC. This method assumes that the columns in the <code>identity_map</code>
   * table were named as specified by {@link IdentifierType#getFieldName}.
   *
   * @param resultSet
   *          a {@link ResultSet} instance that contains an entry for each
   *          identifier type (any of which may be null, apart from the research
   *          ID).
   *
   * @throws SQLException
   *           if an error occurred while retrieving values from the
   *           {@code ResultSet}.
   */
  public IdentityMap(final ResultSet resultSet) throws SQLException {
    this();
    if (resultSet.getString("research_id") != null) {
      identities.put(IdentifierType.ResearchUUID, resultSet.getString("research_id"));
    }
    if (resultSet.getString("huid") != null) {
      identities.put(IdentifierType.HUID, resultSet.getString("huid"));
    }
    if (resultSet.getString("xid") != null) {
      identities.put(IdentifierType.XID, resultSet.getString("xid"));
    }
    final long canvasId = resultSet.getLong("canvas_id");
    if (!resultSet.wasNull()) {
      identities.put(IdentifierType.CanvasID, canvasId);
    }
    final long canvasDataId = resultSet.getLong("canvas_data_id");
    if (!resultSet.wasNull()) {
      identities.put(IdentifierType.CanvasDataID, canvasDataId);
    }
    if (resultSet.getString("eppn") != null) {
      identities.put(IdentifierType.EPPN, resultSet.getString("eppn"));
    }
    if (resultSet.getString("eppn") != null) {
      identities.put(IdentifierType.ActiveDirectoryID, resultSet.getString("active_directory_id"));
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
    fields.add(identities.get(IdentifierType.EPPN));
    fields.add(identities.get(IdentifierType.ActiveDirectoryID));
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
    fields.add(IdentifierType.EPPN.getFieldName());
    fields.add(IdentifierType.ActiveDirectoryID.getFieldName());
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
    final IdentityMap other = (IdentityMap) o;
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

  @Override
  public Map<String, Object> getFieldsAsMap() {
    final Map<String, Object> fields = new HashMap<String, Object>();
    for (final IdentifierType id : IdentifierType.values()) {
      if (identities.containsKey(id)) {
        fields.put(id.getFieldName(), identities.get(id));
      }
    }
    return fields;
  }

  public void setFieldsAsMap(final Map<String, Object> map) {
    for (final String key : map.keySet()) {
      identities.put(IdentifierType.fromFieldName(key), map.get(key));
    }
  }

  public Object get(final IdentifierType idType) {
    return identities.get(idType);
  }

  public void set(final IdentifierType idType, final Object value) {
    identities.put(idType, value);
  }

}
