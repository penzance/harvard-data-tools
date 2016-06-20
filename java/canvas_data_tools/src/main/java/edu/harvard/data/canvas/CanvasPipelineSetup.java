package edu.harvard.data.canvas;

import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.pipeline.DataPipelineGenerator;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPipelineSetup {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException {
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        args[0], true);
    //    final File gitDir = new File(args[1]);
    final String runId = args[2];
    final String host = config.getCanvasDataHost();
    final String key = config.getCanvasApiKey();
    final String secret = config.getCanvasApiSecret();
    final ApiClient api = new ApiClient(host, key, secret);
    setupRun(config, api, runId);
  }

  private static void setupRun(final CanvasDataConfig config, final ApiClient api,
      final String runId) throws DataConfigurationException, UnexpectedApiResponseException,
  IOException, VerificationException {
    final AwsUtils aws = new AwsUtils();
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);

    final DataPipelineGenerator pipeline = new DataPipelineGenerator(config, dataIndex,
        new CanvasCodeManager(), runId);
    pipeline.generate();
  }

}