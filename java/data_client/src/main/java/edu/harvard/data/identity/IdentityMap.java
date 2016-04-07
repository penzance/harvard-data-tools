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

  private String researchId;
  private String huid;
  private String xid;
  private Long canvasId;
  private Long canvasDataId;

  public IdentityMap() {
  }

  public IdentityMap(final CSVRecord record) {
    this.populate(record);
  }

  public IdentityMap(final TableFormat format, final CSVRecord record) {
    this(record);
  }

  public IdentityMap(final ResultSet resultSet) throws SQLException {
    this.populate(resultSet);
  }

  public void populate(final CSVRecord record) {
    this.researchId = record.get(0);
    this.huid = record.get(1);
    this.xid = record.get(2);
    final String $canvasId = record.get(3);
    if ($canvasId != null && $canvasId.length() > 0) {
      this.canvasId = Long.valueOf($canvasId);
    }
    final String $canvasDataId = record.get(4);
    if ($canvasDataId != null && $canvasDataId.length() > 0) {
      this.canvasDataId = Long.valueOf($canvasDataId);
    }
  }

  public void populate(final ResultSet resultSet) throws SQLException {
    this.researchId = resultSet.getString("research_id");
    this.huid = resultSet.getString("huid");
    this.xid = resultSet.getString("xid");
    this.canvasId = resultSet.getLong("canvas_id");
    if (resultSet.wasNull()) {
      this.canvasId = null;
    }
    this.canvasDataId = resultSet.getLong("canvas_data_id");
    if (resultSet.wasNull()) {
      this.canvasDataId = null;
    }
  }

  public String getResearchId() {
    return this.researchId;
  }

  public void setResearchId(final String researchId) {
    this.researchId = researchId;
  }

  public String getHUID() {
    return this.huid;
  }

  public void setHUID(final String huid) {
    this.huid = huid;
  }

  public String getXID() {
    return this.xid;
  }

  public void setXID(final String xid) {
    this.xid = xid;
  }

  public Long getCanvasID() {
    return this.canvasId;
  }

  public void setCanvasID(final Long canvasId) {
    this.canvasId = canvasId;
  }

  public Long getCanvasDataID() {
    return this.canvasDataId;
  }

  public void setCanvasDataID(final Long canvasDataId) {
    this.canvasDataId = canvasDataId;
  }

  @Override
  public List<Object> getFieldsAsList(final TableFormat formatter) {
    final List<Object> fields = new ArrayList<Object>();
    fields.add(researchId);
    fields.add(huid);
    fields.add(xid);
    fields.add(canvasId);
    fields.add(canvasDataId);
    return fields;
  }

  @Override
  public List<String> getFieldNames() {
    final List<String> fields = new ArrayList<String>();
    fields.add("research_id");
    fields.add("huid");
    fields.add("xid");
    fields.add("canvas_id");
    fields.add("canvas_data_id");
    return fields;
  }

  @Override
  public String toString() {
    return "research_id: " + researchId + "\nhuid: " + huid + "\nxid: " + xid + "\ncanvas_id: " + canvasId
        + "\ncanvasDataId: " + canvasDataId;
  }

  @Override
  public int compareTo(final IdentityMap o) {
    return toString().compareTo(o.toString());
  }

  public PreparedStatement getLookupSqlQuery(final Connection connection) throws SQLException {
    final List<String> params = new ArrayList<String>();
    final List<Object> vals = new ArrayList<Object>();
    if (researchId != null) {
      params.add("research_id = ?");
      vals.add(researchId);
    }
    if (huid != null) {
      params.add("huid = ?");
      vals.add(huid);
    }
    if (huid != null) {
      params.add("xid = ?");
      vals.add(xid);
    }
    if (canvasId != null) {
      params.add("canvas_id = ?");
      vals.add(canvasId);
    }
    if (canvasDataId != null) {
      params.add("canvas_data_id = ?");
      vals.add(canvasDataId);
    }

    String query = "SELECT research_id, huid, canvas_id, canvas_data_id FROM " + ID_TABLE_NAME + " WHERE ";
    for (int i=0; i<params.size(); i++) {
      if (i > 0) {
        query += " OR ";
      }
      query += params.get(i);
    }
    query += ";";

    final PreparedStatement statement = connection.prepareStatement(query);
    for (int i=0; i<params.size(); i++) {
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

  @Override
  public Map<String, Object> getFieldsAsMap() {
    final Map<String, Object> fields = new HashMap<String, Object>();
    fields.put("research_id", researchId);
    fields.put("huid", huid);
    fields.put("xid", xid);
    fields.put("canvas_id", canvasId);
    fields.put("canvas_data_id", canvasDataId);
    return fields;
  }
}
