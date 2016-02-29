package edu.harvard.data.client.identity;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.UUID;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.schema.DataSchema;

public class IdentityService {

  private static final String ID_TABLE = "identity_map";
  private final AwsUtils aws;
  private final DataConfiguration config;
  private boolean initialized = false;

  public IdentityService()
      throws IOException, DataConfigurationException {
    this.aws = new AwsUtils();
    this.config = DataConfiguration.getConfiguration("secure.properties");
  }

  public void init() throws SQLException {
    final DataSchema schema = aws.getRedshiftSchema(config);
    if (schema.getTableByName(ID_TABLE) == null) {
      createIdentityTable();
    }
    this.initialized = true;
  }

  private void createIdentityTable() throws SQLException {
    String query = "CREATE TABLE " + ID_TABLE + " (";
    final IdentityType[] values = IdentityType.values();
    for (int i = 0; i < values.length; i++) {
      query += "\n  " + values[i].toString().toLowerCase() + " VARCHAR(256)";
      if (i < values.length - 1) {
        query += ",";
      } else {
        query += ");";
      }
    }
    aws.executeRedshiftQuery(query, config);
  }

  public void populateId(final IndividualIdentity id) throws SQLException {
    if (!initialized) {
      init();
    }
    getIdentities(id);
    if (id.getId(IdentityType.RESEARCH_ID) == null) {
      generateResearchId(id);
      writeId(id);
    }
  }

  private void writeId(final IndividualIdentity id) throws SQLException {
    final String insertQuery = getInsertQuery(id);
    aws.executeRedshiftQuery(insertQuery, config);
  }

  private void generateResearchId(final IndividualIdentity id) {
    final String uuid = UUID.randomUUID().toString();
    // TODO Check for collisions
    id.addIdentity(IdentityType.RESEARCH_ID, uuid);
  }

  private void getIdentities(final IndividualIdentity id) throws SQLException {
    final String selectQuery = getLookupQuery(id);
    final String url = config.getRedshiftUrl();
    try (
        Connection connection = DriverManager.getConnection(url, config.getRedshiftUser(),
            config.getRedshiftPassword());
        Statement st = connection.createStatement();
        ResultSet resultSet = st.executeQuery(selectQuery);) {
      if (resultSet.next()) {
        for (final IdentityType type : IdentityType.values()) {
          final String value = resultSet.getString(type.toString().toLowerCase());
          if (value != null) {
            id.addIdentity(type, value);
          }
        }
        if (resultSet.next()) {
          throw new RuntimeException("TODO: Better exception. Duplicate identity found");
        }
      }
    }
  }

  private static String getLookupQuery(final IndividualIdentity id) {
    String query = "SELECT ";
    final IdentityType[] values = IdentityType.values();
    for (int i = 0; i < values.length; i++) {
      query += values[i].toString().toLowerCase();
      if (i < values.length - 1) {
        query += ", ";
      }
    }
    query += " FROM " + ID_TABLE + " WHERE ";
    final Set<IdentityType> keys = id.getIds().keySet();
    int keyCount = 0;
    for (final IdentityType idType : keys) {
      if (keyCount++ > 0) {
        query += " OR ";
      }
      query += idType.toString().toLowerCase() + " = '" + id.getIds().get(idType) + "'";
    }
    query += ";";
    return query;
  }

  private String getInsertQuery(final IndividualIdentity id) {
    String fields = "";
    String vals = "";
    final IdentityType[] values = IdentityType.values();
    for (int i = 0; i < values.length; i++) {
      final Object value = id.getId(values[i]);
      fields += values[i].toString().toLowerCase();
      if (value == null) {
        vals += "NULL";
      } else if (value instanceof String) {
        vals += value;
      } else {
        vals += "'" + value + "'";
      }
      if (i < values.length - 1) {
        fields += ", ";
        vals += ", ";
      }
    }
    return "INSERT INTO " + ID_TABLE + " (" + fields + ") VALUES (" + vals + ")";
  }

}
