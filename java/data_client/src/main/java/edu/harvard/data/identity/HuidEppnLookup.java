package edu.harvard.data.identity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class HuidEppnLookup {
  private static final int LOOKUP_CHUNK_SIZE = 2;
  private final DataConfig config;

  public HuidEppnLookup(final DataConfig config) {
    this.config = config;
  }

  public void expandIdentities(final List<IdentityMap> identities)
      throws DataConfigurationException, SQLException {
    for (int i = 0; i < identities.size(); i += LOOKUP_CHUNK_SIZE) {
      final int start = i;
      final int end = Math.min(start + LOOKUP_CHUNK_SIZE, identities.size());
      expandIdentities(identities, start, end);
    }
  }

  private void expandIdentities(final List<IdentityMap> identities, final int start, final int end)
      throws DataConfigurationException, SQLException {
    final String view = config.getIdentityOracleSchema() + "." + config.getIdentityOracleView();
    String queryString = "SELECT * FROM " + view + " WHERE huid IN (";
    for (int i = start; i < end; i++) {
      queryString += "?, ";
    }
    queryString = queryString.substring(0, queryString.length() - 2) + ");";

    final String connectionString = config.getIdentityOracleUrl();
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
    } catch (final ClassNotFoundException e) {
      throw new DataConfigurationException("Oracle JDBC driver not found");
    }

    try (
        final Connection connection = DriverManager.getConnection(connectionString,
            config.getIdentityOracleUserName(), config.getIdentityOraclePassword());
        PreparedStatement statement = connection.prepareStatement(queryString);) {
      for (int i = start; i < end; i++) {
        statement.setString(i - start + 1, (String) identities.get(i).get(IdentifierType.HUID));
      }
      try (final ResultSet rs = statement.executeQuery();) {
        while (rs.next()) {
          System.out.println(rs.getString("huid"));
          System.out.println(rs.getString("eppn"));
          System.out.println(rs.getString("adid"));
        }
      }
    }
  }
}
