package edu.harvard.data.matterhorn;

import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.pipeline.InputTableIndex;

public class MatterhornIdentityMapHadoopJob extends IdentityMapHadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final String runId = args[1];
    final AwsUtils aws = new AwsUtils();
    final MatterhornDataConfig config = MatterhornDataConfig.parseInputFiles(MatterhornDataConfig.class,
        configPathString, true);
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);
    new MatterhornIdentityMapHadoopJob(config, dataIndex).run();
  }

  public MatterhornIdentityMapHadoopJob(final MatterhornDataConfig config, final InputTableIndex dataIndex)
      throws IOException, DataConfigurationException {
    super(config, new MatterhornCodeManager(), dataIndex);
  }

}
