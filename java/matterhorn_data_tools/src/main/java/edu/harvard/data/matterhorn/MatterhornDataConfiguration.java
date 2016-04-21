package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.RedshiftConfiguration;

public class MatterhornDataConfiguration implements RedshiftConfiguration {

  private File scratchDir;
  private String redshiftDb;
  private String redshiftHost;
  private String redshiftPort;
  private String redshiftUser;
  private String redshiftPassword;
  private String awsKey;
  private String awsSecretKey;
  private S3ObjectId dropboxBucket;
  private S3ObjectId incomingBucket;

  public static MatterhornDataConfiguration getConfiguration(final String propertiesFileName)
      throws IOException, DataConfigurationException {
    final ClassLoader cl = MatterhornDataConfiguration.class.getClassLoader();
    Properties properties;
    try (final InputStream in = cl.getResourceAsStream(propertiesFileName)) {
      if (in == null) {
        throw new FileNotFoundException(propertiesFileName);
      }
      properties = new Properties();
      properties.load(in);
    }
    final MatterhornDataConfiguration config = new MatterhornDataConfiguration();
    config.scratchDir = new File(getConfigParameter(properties, "scratch_dir"));
    config.redshiftDb = getConfigParameter(properties, "redshift_database");
    config.redshiftHost = getConfigParameter(properties, "redshift_host");
    config.redshiftPort = getConfigParameter(properties, "redshift_port");
    config.redshiftUser = getConfigParameter(properties, "redshift_user");
    config.redshiftPassword = getConfigParameter(properties, "redshift_password");
    config.awsKey = getConfigParameter(properties, "aws_key_id");
    config.awsSecretKey = getConfigParameter(properties, "aws_secret_key");
    config.incomingBucket = new S3ObjectId(getConfigParameter(properties, "incoming_data_bucket"),
        "/");
    config.dropboxBucket = new S3ObjectId(getConfigParameter(properties, "dropbox_data_bucket"),
        "/");
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

  private MatterhornDataConfiguration() {
  }

  public File getScratchDir() {
    return scratchDir;
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
  public String getRedshiftUrl() {
    return "jdbc:postgresql://" + redshiftHost + ":" + redshiftPort + "/" + redshiftDb;
  }

  @Override
  public String getAwsKey() {
    return awsKey;
  }

  @Override
  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  public S3ObjectId getDropboxBucket() {
    return dropboxBucket;
  }

  public S3ObjectId getIncomingBucket() {
    return incomingBucket;
  }

}
