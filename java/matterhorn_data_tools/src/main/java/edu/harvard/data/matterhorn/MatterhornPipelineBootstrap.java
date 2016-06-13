package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.pipeline.DataPipelineGenerator;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class MatterhornPipelineBootstrap {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException {
    final String configPath = args[0];
    final File gitDir = new File(args[1]);
    final String runId = args[2];

    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPath, true);
    final S3ObjectId dumpLocation = AwsUtils.key(config.getS3IncomingLocation(), "TestData");
    final MatterhornCodeGenerator generator = new MatterhornCodeGenerator("1.0", gitDir,
        null, config, null);

    final GenerationSpec spec = generator.createGenerationSpec();
    final DataPipelineGenerator pipeline = new DataPipelineGenerator("Matterhorn", spec, config,
        dumpLocation, new MatterhornCodeManager(), runId);
    pipeline.generate();
  }

}
