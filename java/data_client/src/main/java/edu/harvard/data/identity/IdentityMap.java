package edu.harvard.data.identity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

// Create Redshift table using:
// CREATE TABLE identity_map (
//     research_id VARCHAR(255),
//     huid VARCHAR(255),
//     canvas_id BIGINT,
//     canvas_data_id BIGINT
// );

public class IdentityMap implements DataTable, Comparable<IdentityMap> {

  private static final String ID_TABLE_NAME = "identity_map";

  private String researchId;
  private String huid;
  private Long canvasId;
  private Long canvasDataId;

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
    final String $canvasId = record.get(2);
    if ($canvasId != null && $canvasId.length() > 0) {
      this.canvasId = Long.valueOf($canvasId);
    }
    final String $canvasDataId = record.get(3);
    if ($canvasDataId != null && $canvasDataId.length() > 0) {
      this.canvasDataId = Long.valueOf($canvasDataId);
    }
  }

  public void populate(final ResultSet resultSet) throws SQLException {
    this.researchId = resultSet.getString("research_id");
    this.huid = resultSet.getString("huid");
    this.canvasId = resultSet.getLong("canvas_id");
    if (resultSet.wasNull()) {
      this.canvasId = null;
    }
    this.canvasDataId = resultSet.getLong("canvas_data_id");
    if (resultSet.wasNull()) {
      this.canvasDataId = null;
    }
  }

  public IdentityMap(final String researchId, final String huid, final Long canvasId,
      final Long canvasDataId) {
    this.researchId = researchId;
    this.huid = huid;
    this.canvasId = canvasId;
    this.canvasDataId = canvasDataId;
  }

  public String getResearchId() {
    return this.researchId;
  }

  public void setResearchId(final String researchId) {
    this.researchId = researchId;
  }

  public String getHuid() {
    return this.huid;
  }

  public void setHuid(final String huid) {
    this.huid = huid;
  }

  public Long getCanvasId() {
    return this.canvasId;
  }

  public void setCanvasId(final Long canvasId) {
    this.canvasId = canvasId;
  }

  public Long getCanvasDataId() {
    return this.canvasDataId;
  }

  public void setCanvasDataId(final Long canvasDataId) {
    this.canvasDataId = canvasDataId;
  }

  @Override
  public List<Object> getFieldsAsList(final TableFormat formatter) {
    final List<Object> fields = new ArrayList<Object>();
    fields.add(researchId);
    fields.add(huid);
    fields.add(canvasId);
    fields.add(canvasDataId);
    return fields;
  }

  public static List<String> getFieldNames() {
    final List<String> fields = new ArrayList<String>();
    fields.add("research_id");
    fields.add("huid");
    fields.add("canvas_id");
    fields.add("canvas_data_id");
    return fields;
  }

  @Override
  public String toString() {
    return "research_id: " + researchId + "\nhuid: " + huid + "\ncanvas_id: " + canvasId
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
}
