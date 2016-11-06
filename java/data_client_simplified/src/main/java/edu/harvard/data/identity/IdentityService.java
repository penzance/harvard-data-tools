package edu.harvard.data.identity;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.S3TableReader;
import edu.harvard.data.io.TableReader;

public class IdentityService implements Closeable {

  private final DataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId identityMapLocation;
  private final TableFormat identityMapFormat;
  private final IdentifierType mainIdentifier;
  private final Map<Object, IdentityMap> identityMap;

  public IdentityService(final DataConfig config, final S3ObjectId workingLocation)
      throws SQLException, IOException {
    this.config = config;
    this.aws = new AwsUtils();
    this.identityMapLocation = AwsUtils.key(workingLocation, "unloaded_tables/identity_map");
    this.mainIdentifier = config.getMainIdentifier();
    this.identityMap = new HashMap<Object, IdentityMap>();
    this.identityMapFormat = new FormatLibrary().getFormat(Format.DecompressedInternal);
    setup();
  }

  public void setup() throws SQLException, IOException {
    unloadIdentityMap();
    readIdentityMap();
  }

  private void unloadIdentityMap() throws SQLException {
    aws.deleteDirectory(identityMapLocation);
    aws.unloadTable("pii.identity_map", identityMapLocation, config);
  }

  private void readIdentityMap() throws IOException {
    final File tmpDir = new File(config.getScratchDir() + "/id_map");
    for (final S3ObjectSummary idMapFile : aws.listKeys(identityMapLocation)) {
      final S3ObjectId idFileLocation = AwsUtils.key(idMapFile);
      try (TableReader<IdentityMap> in = new S3TableReader<IdentityMap>(aws, IdentityMap.class,
          identityMapFormat, idFileLocation, tmpDir)) {
        for (final IdentityMap id : in) {
          identityMap.put(id.get(mainIdentifier), id);
        }
      }
    }
    System.out.println("Read " + identityMap.size() + " identifiers");
  }

  public String getResearchUuid(final IdentityMap idMap, final IdentifierType mainIdentifier) {
    final Object mainId = idMap.get(mainIdentifier);
    synchronized(idMap) {
      if (!identityMap.containsKey(mainId)) {
        idMap.set(IdentifierType.ResearchUUID, UUID.randomUUID().toString());
        identityMap.put(mainId, idMap);
      }
      return (String) identityMap.get(mainId).get(IdentifierType.ResearchUUID);
    }
  }

  @Override
  public void close() throws IOException {
    // TODO: Save ID map back to Redshift
  }
}
