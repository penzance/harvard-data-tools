package edu.harvard.data.matterhorn;

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

public class MatterhornPhase0Bootstrap extends Phase0Bootstrap implements RequestHandler<String, String> {

  @Override
  public String handleRequest(final String configPathString, final Context context) {
    try {
      super.init(configPathString, MatterhornDataConfig.class);
      super.run();
    } catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
      return "Error: " + e.getMessage();
    }
    return "";
  }

  @Override
  protected List<S3ObjectId> getInfrastructureConfigPaths() {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final S3ObjectId configPath = AwsUtils.key(config.getCodeBucket(), config.getGitTagOrBranch());
    paths.add(AwsUtils.key(configPath, "tiny_phase_0.properties"));
    paths.add(AwsUtils.key(configPath, "tiny_emr.properties"));
    return paths;
  }

  @Override
  protected Map<String, String> getCustomEc2Environment() {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("DATA_SET_ID", runId);
    env.put("DATA_SCHEMA_VERSION", "1.0"); // XXX: Remove
    return env;
  }

  @Override
  protected boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    return true;
  }

}
