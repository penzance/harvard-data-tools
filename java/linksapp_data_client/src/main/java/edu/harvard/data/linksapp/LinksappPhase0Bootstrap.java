package edu.harvard.data.linksapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.pipeline.Phase0Bootstrap;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class LinksappPhase0Bootstrap extends Phase0Bootstrap
implements RequestHandler<BootstrapParameters, String> {

  @Override
  public String handleRequest(final BootstrapParameters params, final Context context) {
    try {
      super.init(params.getConfigPathString(), LinksappDataConfig.class, false);
      super.run(context);
    } catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
      return "Error: " + e.getMessage();
    }
    return "";
  }

  @Override
  protected List<S3ObjectId> getInfrastructureConfigPaths() {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final S3ObjectId configPath = AwsUtils.key(config.getCodeBucket(), "infrastructure");
    paths.add(AwsUtils.key(configPath, "app_phase_0.properties"));
    paths.add(AwsUtils.key(configPath, "app_emr.properties"));
    return paths;
  }

  @Override
  protected Map<String, String> getCustomEc2Environment() {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("DATA_SET_ID", runId);
    env.put("DATA_SCHEMA_VERSION", "1.0");
    return env;
  }

  @Override
  protected boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    return !aws.listKeys(((LinksappDataConfig)config).getDropboxBucket()).isEmpty();
  }

  @Override
  protected void setup()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
  }

}
