package edu.harvard.data.client.redshift;

import java.sql.ResultSet;
import java.sql.SQLException;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaType;

public class RedshiftColumn extends DataSchemaColumn {

  private final String name;
  private final DataSchemaType type;
  private Integer length;

  protected RedshiftColumn(final ResultSet rs) throws SQLException {
    super(false);
    this.name = rs.getString("column_name");
    this.type = DataSchemaType.parse(rs.getString("data_type"));
    if (this.type == DataSchemaType.VarChar) {
      this.length = rs.getInt("character_maximum_length");
    } else {
      this.length = rs.getInt("numeric_precision");
    }
  }

  public RedshiftColumn(final RedshiftColumn original) {
    super(original.newlyGenerated);
    this.name = original.name;
    this.type = original.type;
    this.length = original.length;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return "";
  }

  @Override
  public DataSchemaType getType() {
    return type;
  }

  @Override
  public Integer getLength() {
    return length;
  }

  @Override
  public DataSchemaColumn copy() {
    return new RedshiftColumn(this);
  }

  @Override
  public String toString() {
    return name + ": " + type + " (" + length + ") " + (newlyGenerated ? "*":"");
  }
}
