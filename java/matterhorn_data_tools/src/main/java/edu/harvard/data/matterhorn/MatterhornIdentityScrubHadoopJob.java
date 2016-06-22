package edu.harvard.data.matterhorn;

import java.io.IOException;
import java.net.URISyntaxException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.identity.IdentityScrubHadoopJob;
import edu.harvard.data.leases.LeaseRenewalException;
import edu.harvard.data.pipeline.InputTableIndex;

public class MatterhornIdentityScrubHadoopJob extends IdentityScrubHadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  LeaseRenewalException, InstantiationException, IllegalAccessException, NoInputDataException,
  URISyntaxException, ClassNotFoundException, InterruptedException {
    final String configPathString = args[0];
    final String runId = args[1];
    final AwsUtils aws = new AwsUtils();
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);
    new MatterhornIdentityScrubHadoopJob(config, dataIndex, runId).run();
  }

  public MatterhornIdentityScrubHadoopJob(final MatterhornDataConfig config,
      final InputTableIndex dataIndex, final String runId)
          throws IOException, DataConfigurationException, URISyntaxException {
    super(config, new MatterhornCodeManager(), dataIndex, runId);
  }

}
