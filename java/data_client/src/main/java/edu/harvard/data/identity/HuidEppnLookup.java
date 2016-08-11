package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public class HuidEppnLookup {

  private static final Logger log = LogManager.getLogger();

  private static final int LOOKUP_CHUNK_SIZE = 100;
  private final DataConfig config;
  private final ArrayList<IdentityMap> identities;
  private final HashMap<String, IdentityMap> unknownEppn;
  private final HashMap<String, IdentityMap> unknownHuid;
  private final HadoopUtilities hadoopUtils;
  private final Configuration hadoopConfig;
  private final TableFormat format;

  public HuidEppnLookup(final DataConfig config, final TableFormat format) {
    this.config = config;
    this.identities = new ArrayList<IdentityMap>();
    this.unknownEppn = new HashMap<String, IdentityMap>();
    this.unknownHuid = new HashMap<String, IdentityMap>();
    this.hadoopUtils = new HadoopUtilities();
    this.hadoopConfig = new Configuration();
    this.format = format;
  }

  public void expandIdentities(final URI[] inputPaths, final URI outputPath)
      throws SQLException, DataConfigurationException, IOException {
    readIdentities(inputPaths);
    log.info("Found " + unknownEppn.size() + " unknown EPPNs and " + unknownHuid.size()
    + " unknown HUIDs");
    try (Connection connection = establishConnection()) {
      if (!unknownEppn.isEmpty()) {
        expand(unknownEppn, IdentifierType.HUID, IdentifierType.EPPN, connection);
      }
      if (!unknownHuid.isEmpty()) {
        expand(unknownHuid, IdentifierType.EPPN, IdentifierType.HUID, connection);
      }
    }
    writeIdentities(outputPath);
  }

  private Connection establishConnection() throws DataConfigurationException, SQLException {
    final String connectionString = config.getIdentityOracleUrl();
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
    } catch (final ClassNotFoundException e) {
      throw new DataConfigurationException("Oracle JDBC driver not found");
    }
    return DriverManager.getConnection(connectionString, config.getIdentityOracleUserName(),
        config.getIdentityOraclePassword());
  }

  private void readIdentities(final URI[] paths) throws IOException {
    try (TableReader<IdentityMap> in = hadoopUtils.getHdfsTableReader(hadoopConfig, paths, format,
        IdentityMap.class)) {
      for (final IdentityMap id : in) {
        identities.add(id);
        final String huid = (String) id.get(IdentifierType.HUID);
        final String eppn = (String) id.get(IdentifierType.EPPN);
        if (huid != null && eppn == null) {
          log.info("Unknown EPPN for HUID " + huid);
          unknownEppn.put(huid, id);
        }
        if (eppn != null && huid == null) {
          log.info("Unknown HUID for EPPN " + huid);
          unknownHuid.put(eppn, id);
        }
      }
    }
  }

  private void writeIdentities(final URI outputPath) throws IOException {
    try (TableWriter<IdentityMap> out = hadoopUtils.getHdfsTableWriter(hadoopConfig, outputPath,
        format, IdentityMap.class)) {
      for (final IdentityMap id : identities) {
        out.add(id);
      }
    }
  }

  private void expand(final Map<String, IdentityMap> unknownIdentities,
      final IdentifierType knownField, final IdentifierType unknownField,
      final Connection connection) throws DataConfigurationException, SQLException {
    final List<IdentityMap> idMaps = new ArrayList<IdentityMap>(unknownIdentities.values());
    for (int i = 0; i < idMaps.size(); i += LOOKUP_CHUNK_SIZE) {
      final int start = i;
      final int end = Math.min(start + LOOKUP_CHUNK_SIZE, idMaps.size());
      populateUnknownIdentities(idMaps, unknownIdentities, start, end, knownField, unknownField,
          connection);
      return;
    }
  }

  private void populateUnknownIdentities(final List<IdentityMap> idMaps,
      final Map<String, IdentityMap> unknownIdentities, final int start, final int end,
      final IdentifierType knownField, final IdentifierType unknownField,
      final Connection connection) throws DataConfigurationException, SQLException {
    final String view = config.getIdentityOracleSchema() + "." + config.getIdentityOracleView();
    String queryString = "SELECT huid,eppn,adid FROM " + view + " WHERE "
        + knownField.getFieldName() + " IN (";
    for (int i = start; i < end; i++) {
      queryString += "?, ";
    }
    queryString = queryString.substring(0, queryString.length() - 2) + ")";

    try (PreparedStatement statement = connection.prepareStatement(queryString)) {
      for (int i = start; i < end; i++) {
        statement.setString(i - start + 1, (String) identities.get(i).get(knownField));
      }
      try (final ResultSet rs = statement.executeQuery();) {
        for (int i = start; i < end; i++) {
          while (rs.next()) {
            final String knownValue = rs.getString(knownField.getFieldName());
            final String unknownValue = rs.getString(unknownField.getFieldName());
            if (knownValue == null) {
              throw new RuntimeException("Query returned unexpected key " + knownValue);
            }
            log.info("Unknown identities: " + unknownIdentities);
            final IdentityMap id = unknownIdentities.get(knownValue);
            log.info("Updating ID with known value: " + knownValue + ". ID map: " + id);
            id.set(unknownField, unknownValue);
            id.set(IdentifierType.ActiveDirectoryID, rs.getString("adid"));
          }
        }
      }
    }
  }
}
