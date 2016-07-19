package edu.harvard.data;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.pipeline.PipelineCompletionMessage;
import edu.harvard.data.pipeline.PipelineExecutionRecord;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public abstract class Phase0 {

  public static void main(final String[] args) throws ClassNotFoundException,
  InstantiationException, IllegalAccessException, IOException, DataConfigurationException {
    final String configPathString = args[0];
    final String runId = args[1];
    final String datasetId = args[2];
    final int threads = Integer.parseInt(args[3]);
    final String codeManagerClassName = args[4];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
    final DataConfig config = codeManager.getDataConfig(configPathString, false);

    PipelineExecutionRecord.init(config.getPipelineDynamoTable());
    final PipelineExecutionRecord record = PipelineExecutionRecord.find(runId);
    record.setPhase0Start(new Date());
    record.save();

    ReturnStatus status;
    ExecutorService exec = null;
    try {
      exec = Executors.newFixedThreadPool(threads);
      final Phase0 phase0 = codeManager.getPhase0(configPathString, datasetId, runId, exec);
      status = phase0.run();
      record.setPhase0Success(true);
    } catch (final IOException e) {
      cleanup(config, record, e);
      status = ReturnStatus.IO_ERROR;
    } catch (final DataConfigurationException e) {
      cleanup(config, record, e);
      status = ReturnStatus.CONFIG_ERROR;
    } catch (final InterruptedException e) {
      cleanup(config, record, e);
      status = ReturnStatus.UNKNOWN_ERROR;
    } catch (final ExecutionException e) {
      cleanup(config, record, e);
      status = ReturnStatus.UNKNOWN_ERROR;
    } catch (final UnexpectedApiResponseException e) {
      cleanup(config, record, e);
      status = ReturnStatus.API_ERROR;
    } catch (final VerificationException e) {
      cleanup(config, record, e);
      status = ReturnStatus.VERIFICATION_FAILURE;
    } catch (final ArgumentError e) {
      cleanup(config, record, e);
      status = ReturnStatus.ARGUMENT_ERROR;
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
    record.save();
    System.exit(status.getCode());
  }

  private static void cleanup(final DataConfig config, final PipelineExecutionRecord record,
      final Exception e) throws JsonProcessingException {
    e.printStackTrace();
    record.setPhase0Success(false);

    final PipelineCompletionMessage success = new PipelineCompletionMessage(null, record.getRunId(),
        config.getReportBucket(), config.getFailureSnsArn(), config.getPipelineDynamoTable());
    final String msg = new ObjectMapper().writeValueAsString(success);
    final AmazonSNSClient sns = new AmazonSNSClient();
    final String topicArn = config.getCompletionSnsArn();
    final PublishRequest publishRequest = new PublishRequest(topicArn, msg);
    sns.publish(publishRequest);
    System.out.println("Published to " + topicArn);
  }

  protected abstract ReturnStatus run()
      throws IOException, InterruptedException, ExecutionException, DataConfigurationException,
      UnexpectedApiResponseException, VerificationException, ArgumentError;

}
