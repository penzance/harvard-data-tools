package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.TableFormat;
import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.FileTableWriter;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public class VerificationPeople {

  private static final Logger log = LogManager.getLogger();

  private static final int MAX_SAMPLE_FILES = 5;
  private static final int MAX_INTERESTING_PEOPLE = 500;

  private final String inputDir;
  private final Random random;
  private final TableFormat format;
  private final Map<Long, Set<String>> userOperations;
  private final FileSystem fs;
  private List<Long> people;
  private final Map<Long, IdentityMap> originalIdentities;

  private final DataConfiguration dataConfig;

  public VerificationPeople(final DataConfiguration dataConfig, final Configuration hadoopConfig,
      final URI hdfsService, final String inputDir, final TableFormat format) throws IOException {
    this.dataConfig = dataConfig;
    this.inputDir = inputDir;
    this.format = format;
    userOperations = new HashMap<Long, Set<String>>();
    this.random = new Random(12345); // Seed the random generator for testing.
    this.fs = FileSystem.get(hdfsService, hadoopConfig);
    this.originalIdentities = new HashMap<Long, IdentityMap>();
  }

  public void storeInterestingIds(final String interestingIdFile) throws IOException, SQLException {
    findInterestingIds();
    getIdMapsFromRedshift(dataConfig);
    writeIds(interestingIdFile);
  }

  private void findInterestingIds() throws IOException {
    final Long start = System.currentTimeMillis();
    people = new ArrayList<Long>();

    for (final Path path : calculatePaths(inputDir + "/requests")) {
      scanRequestTable(path);
    }

    selectPeople();
    final Long time = (System.currentTimeMillis() - start) / 1000;
    log.info("Found interesting people in " + time + " seconds.");
  }

  // Pick the MAX_INTERESTING_PEOPLE number of people who have touched the most
  // handlers
  private void selectPeople() {
    final TreeMap<Long, Integer> peopleByNumOperations = new TreeMap<Long, Integer>();
    for (final Entry<Long, Set<String>> entry : userOperations.entrySet()) {
      peopleByNumOperations.put(entry.getKey(), entry.getValue().size());
    }
    final Iterator<Long> maxKeys = peopleByNumOperations.descendingKeySet().iterator();
    for (int i = 0; i < MAX_INTERESTING_PEOPLE; i++) {
      if (maxKeys.hasNext()) {
        final Long person = maxKeys.next();
        people.add(person);
      }
    }
  }

  private void scanRequestTable(final Path path) throws IOException {
    log.info("Scanning request table at path " + path);
    try (TableReader<Phase0Requests> in = new FileTableReader<Phase0Requests>(Phase0Requests.class,
        format, fs.open(path));) {
      for (final Phase0Requests request : in) {
        processRequest(request);
      }
    }
  }

  private void processRequest(final Phase0Requests request) {
    final Long userId = request.getUserId();
    final String controller = request.getWebApplicationController();
    final String action = request.getWebApplicaitonAction();
    if (userId != null && controller != null && action != null) {
      final String operation = controller + "#" + action;
      if (!userOperations.containsKey(userId)) {
        userOperations.put(userId, new HashSet<String>());
      }
      userOperations.get(userId).add(operation);
    }
  }

  // Grab the last file, plus up to MAX_SAMPLE_FILES - 1 random other files.
  private Set<Path> calculatePaths(final String dir) throws IOException {
    final FileStatus[] fileStatus = fs.listStatus(new Path(dir));
    final List<String> paths = new ArrayList<String>();
    for (final FileStatus status : fileStatus) {
      paths.add(status.getPath().toString());
    }
    Collections.sort(paths);

    final Set<Path> chosen = new HashSet<Path>();
    chosen.add(new Path(paths.remove(paths.size() - 1)));
    while (!paths.isEmpty() && chosen.size() < MAX_SAMPLE_FILES) {
      chosen.add(new Path(paths.remove(random.nextInt(paths.size() - 1))));
    }
    for (final Path p : chosen) {
      log.info("Selecting " + p + " to search for interesting users");
    }
    return chosen;
  }

  private void getIdMapsFromRedshift(final DataConfiguration config) throws SQLException {
    final Long start = System.currentTimeMillis();
    final String url = config.getRedshiftUrl();
    String queryString = "SELECT * FROM identity_map WHERE identity_map.canvas_data_id IN (";
    for (int i = 0; i < people.size(); i++) {
      queryString += "?, ";
    }
    queryString = queryString.substring(0, queryString.length() - 2) + ");";
    log.info("Executing query \n" + queryString + "\n on " + url);
    try (
        Connection connection = DriverManager.getConnection(url, config.getRedshiftUser(),
            config.getRedshiftPassword());
        PreparedStatement statement = connection.prepareStatement(queryString);) {
      for (int i = 0; i < people.size(); i++) {
        statement.setLong(i + 1, people.get(i));
      }
      try (final ResultSet rs = statement.executeQuery();) {
        while (rs.next()) {
          final IdentityMap id = new IdentityMap(rs);
          originalIdentities.put(id.getCanvasDataID(), id);
        }
      }
    }
    log.info("Downloaded identities for " + originalIdentities.size() + " of " + people.size()
    + " individuals.");
    final Long time = (System.currentTimeMillis() - start) / 1000;
    log.info("Got identities in " + time + " seconds.");
  }

  public void writeIds(final String output) throws IOException {
    final Set<Long> unknownPeople = new HashSet<Long>(people);
    try (final OutputStream outStream = fs.create(new Path(output));
        TableWriter<IdentityMap> out = new FileTableWriter<IdentityMap>(IdentityMap.class, format,
            "identity_map", outStream)) {
      for (final IdentityMap id : originalIdentities.values()) {
        unknownPeople.remove(id.getCanvasDataID());
        out.add(id);
      }
      for (final Long canvasDataId : unknownPeople) {
        final IdentityMap id = new IdentityMap();
        id.setCanvasDataID(canvasDataId);
        out.add(id);
      }
    }
    log.info("Wrote " + originalIdentities.size() + " known IDs and " + unknownPeople.size()
    + " unknown IDs to " + output);
  }

  public List<Long> getInterestingIds() {
    return people;
  }

}
