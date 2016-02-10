package edu.harvard.data.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.services.s3.model.S3ObjectId;

public class DataConfiguration {

  private String canvasApiKey;
  private String canvasApiSecret;
  private String canvasDataHost;
  private File scratchDir;
  private S3ObjectId canvasDataArchiveKey;
  private String dumpInfoDynamoTable;

  public static DataConfiguration getConfiguration(final String propertiesFileName)
      throws IOException, DataConfigurationException {
    final ClassLoader cl = DataConfiguration.class.getClassLoader();
    Properties properties;
    try (final InputStream in = cl.getResourceAsStream(propertiesFileName)) {
      if (in == null) {
        throw new FileNotFoundException(propertiesFileName);
      }
      properties = new Properties();
      properties.load(in);
    }
    final DataConfiguration config = new DataConfiguration();
    config.canvasApiKey = getConfigParameter(properties, "canvas_data_api_key");
    config.canvasApiSecret = getConfigParameter(properties, "canvas_data_api_secret");
    config.canvasDataHost = getConfigParameter(properties, "canvas_data_host");
    config.scratchDir = new File(getConfigParameter(properties, "scratch_dir"));
    final String dataBucket = getConfigParameter(properties, "canvas_data_bucket");
    config.canvasDataArchiveKey = new S3ObjectId(dataBucket,
        getConfigParameter(properties, "canvas_data_archive_key"));
    config.setDumpInfoDynamoTable(getConfigParameter(properties, "dump_info_dynamo_table"));
    return config;
  }

  private static String getConfigParameter(final Properties properties, final String key)
      throws DataConfigurationException {
    final String param = (String) properties.get(key);
    if (param == null) {
      throw new DataConfigurationException("Configuration file missing " + key);
    }
    return param;
  }

  private DataConfiguration() {
  }

  public String getCanvasApiKey() {
    return canvasApiKey;
  }

  public String getCanvasApiSecret() {
    return canvasApiSecret;
  }

  public String getCanvasDataHost() {
    return canvasDataHost;
  }

  public File getScratchDir() {
    return scratchDir;
  }

  public S3ObjectId getCanvasDataArchiveKey() {
    return canvasDataArchiveKey;
  }

  public String getDumpInfoDynamoTable() {
    return dumpInfoDynamoTable;
  }

  public void setDumpInfoDynamoTable(final String dumpInfoDynamoTable) {
    this.dumpInfoDynamoTable = dumpInfoDynamoTable;
  }

}
