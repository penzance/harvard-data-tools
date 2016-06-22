package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.pipeline.InputTableIndex;

public class S3ToHdfsManifestGenerator {
  private final File dir;
  private final InputTableIndex dataIndex;
  private final DataConfig config;
  private final ObjectMapper jsonMapper;

  public S3ToHdfsManifestGenerator(final File dir, final DataConfig config,
      final InputTableIndex dataIndex) {
    this.config = config;
    this.dir = dir;
    this.dataIndex = dataIndex;
    this.jsonMapper = new ObjectMapper();
  }

  public void generate() throws IOException {
    final File manifestFile = new File(dir, config.getS3ToHdfsManifestFile());

    try (final PrintStream out = new PrintStream(
        new GZIPOutputStream(new FileOutputStream(manifestFile)))) {
      for (final String table : dataIndex.getTableNames()) {
        generateManifestEntries(out, table);
      }
    }
  }

  private void generateManifestEntries(final PrintStream out, final String table)
      throws JsonGenerationException, JsonMappingException, IOException {
    for (final S3ObjectId file : dataIndex.getFiles(table)) {
      final S3ToHdfsManifestLine line = new S3ToHdfsManifestLine();
      final String[] keyParts = file.getKey().split("/");
      line.path = AwsUtils.uri(file);
      line.baseName = keyParts[keyParts.length - 2] + "/" + keyParts[keyParts.length - 1];
      line.srcDir = "s3://" + file.getBucket();
      for (int i=0; i<keyParts.length - 2; i++) {
        line.srcDir += "/" + keyParts[i];
      }
      line.size = dataIndex.getFileSize(file);
      out.println(jsonMapper.writeValueAsString(line));
    }
  }
}

class S3ToHdfsManifestLine {
  public String path;
  public String baseName;
  public String srcDir;
  public long size;
}
