package edu.harvard.data.matterhorn;

import java.io.IOException;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.pipeline.DataPipelineGenerator;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class MatterhornPipelineSetup {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException {
    final String configPath = args[0];
    //    final File gitDir = new File(args[1]);
    final String runId = args[2];
    //    final String datasetId = args[3];

    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPath, true);
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = aws.readJson(config.getIndexFileS3Location(runId),
        InputTableIndex.class);
    final DataPipelineGenerator pipeline = new DataPipelineGenerator(config,
        dataIndex, new MatterhornCodeManager(), runId);
    pipeline.generate();
  }

}
