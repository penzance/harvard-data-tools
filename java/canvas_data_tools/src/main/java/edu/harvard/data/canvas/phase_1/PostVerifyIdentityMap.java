package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.io.HdfsTableReader;

public class PostVerifyIdentityMap implements Verifier {
  private static final Logger log = LogManager.getLogger();
  private final String originalDir;
  private final String updatedDir;
  private final URI hdfsService;
  private final Configuration hadoopConfig;
  private final TableFormat format;

  public PostVerifyIdentityMap(final Configuration hadoopConfig, final URI hdfsService,
      final String originalDir, final String updatedDir, final TableFormat format) {
    this.hadoopConfig = hadoopConfig;
    this.hdfsService = hdfsService;
    this.originalDir = originalDir;
    this.updatedDir = updatedDir;
    this.format = format;
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info(
        "Verifying identity map. Original dir: " + originalDir + ", updated dir: " + updatedDir);
    final Map<Long, String> originalIds = readIdMap(originalDir);
    final Map<Long, String> updatedIds = readIdMap(updatedDir);

    for (final Entry<Long, String> entry : originalIds.entrySet()) {
      if (updatedIds.containsKey(entry.getKey())) {
        final String updatedRid = updatedIds.get(entry.getKey());
        if (!updatedRid.equals(entry.getValue())) {
          throw new VerificationException("Canvas data ID " + entry.getKey() + " should map to "
              + entry.getValue() + " but actually mapped to Research ID " + updatedRid + "");
        }
      }
    }
  }

  private Map<Long, String> readIdMap(final String dir) throws IOException {
    final FileSystem fs = FileSystem.get(hdfsService, hadoopConfig);
    final Map<Long, String> ids = new HashMap<Long, String>();
    for (final Path path : HadoopJob.listFiles(hdfsService, dir)) {
      try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
          format, fs, path)) {
        log.info("Loading IDs from path " + path);
        for (final IdentityMap id : in) {
          ids.put((Long) id.get(IdentifierType.CanvasDataID),
              (String) id.get(IdentifierType.ResearchUUID));
        }
      }
    }
    return ids;
  }

}
