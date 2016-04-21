package edu.harvard.data.canvas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.RedshiftConfiguration;

public class CanvasDataConfiguration implements RedshiftConfiguration {

  private String canvasApiKey;
  private String canvasApiSecret;
  private String canvasDataHost;
  private File scratchDir;
  private S3ObjectId canvasDataArchiveKey;
  private String dumpInfoDynamoTable;
  private String redshiftDb;
  private String redshiftHost;
  private String redshiftPort;
  private String redshiftUser;
  private String redshiftPassword;
  private String tableInfoDynamoTable;
  private String awsKey;
  private String awsSecretKey;

  public static CanvasDataConfiguration getConfiguration(final String propertiesFileName)
      throws IOException, DataConfigurationException {
    final ClassLoader cl = CanvasDataConfiguration.class.getClassLoader();
    Properties properties;
    try (final InputStream in = cl.getResourceAsStream(propertiesFileName)) {
      if (in == null) {
        throw new FileNotFoundException(propertiesFileName);
      }
      properties = new Properties();
      properties.load(in);
    }
    final CanvasDataConfiguration config = new CanvasDataConfiguration();
    config.canvasApiKey = getConfigParameter(properties, "canvas_data_api_key");
    config.canvasApiSecret = getConfigParameter(properties, "canvas_data_api_secret");
    config.canvasDataHost = getConfigParameter(properties, "canvas_data_host");
    config.scratchDir = new File(getConfigParameter(properties, "scratch_dir"));
    final String dataBucket = getConfigParameter(properties, "incoming_data_bucket");
    config.canvasDataArchiveKey = new S3ObjectId(dataBucket,
        getConfigParameter(properties, "canvas_data_archive_key"));
    config.dumpInfoDynamoTable = getConfigParameter(properties, "dump_info_dynamo_table");
    config.tableInfoDynamoTable = getConfigParameter(properties, "table_info_dynamo_table");
    config.redshiftDb = getConfigParameter(properties, "redshift_database");
    config.redshiftHost = getConfigParameter(properties, "redshift_host");
    config.redshiftPort = getConfigParameter(properties, "redshift_port");
    config.redshiftUser = getConfigParameter(properties, "redshift_user");
    config.redshiftPassword = getConfigParameter(properties, "redshift_password");
    config.awsKey = getConfigParameter(properties, "aws_key_id");
    config.awsSecretKey = getConfigParameter(properties, "aws_secret_key");
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

  private CanvasDataConfiguration() {
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

  public String getTableInfoDynamoTable() {
    return tableInfoDynamoTable;
  }

  @Override
  public String getRedshiftHost() {
    return redshiftHost;
  }

  @Override
  public String getRedshiftPort() {
    return redshiftPort;
  }

  @Override
  public String getRedshiftDatabase() {
    return redshiftDb;
  }

  @Override
  public String getRedshiftUser() {
    return redshiftUser;
  }

  @Override
  public String getRedshiftPassword() {
    return redshiftPassword;
  }

  @Override
  public String getAwsKey() {
    return awsKey;
  }

  @Override
  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  @Override
  public String getRedshiftUrl() {
    return "jdbc:postgresql://" + redshiftHost + ":" + redshiftPort + "/" + redshiftDb;
  }

}
