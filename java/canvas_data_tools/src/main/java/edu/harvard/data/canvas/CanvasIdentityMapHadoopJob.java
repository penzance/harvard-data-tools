package edu.harvard.data.canvas;

import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.pipeline.InputTableIndex;

public class CanvasIdentityMapHadoopJob extends IdentityMapHadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final String runId = args[1];
    final AwsUtils aws = new AwsUtils();
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, true);
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);
    new CanvasIdentityMapHadoopJob(config, dataIndex).run();
  }

  public CanvasIdentityMapHadoopJob(final CanvasDataConfig config, final InputTableIndex dataIndex)
      throws IOException, DataConfigurationException {
    super(config, new CanvasCodeManager(), dataIndex);
  }

}
