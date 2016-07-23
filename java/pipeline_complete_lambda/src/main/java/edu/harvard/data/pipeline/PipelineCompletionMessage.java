package edu.harvard.data.pipeline;

public class PipelineCompletionMessage {
  private final String pipelineId;
  private final String reportBucket;
  private final String snsArn;
  private final String pipelineDynamoTable;
  private final String runId;
  private final String source;

  public PipelineCompletionMessage(final String pipelineId, final String runId, final String reportBucket,
      final String snsArn, final String pipelineDynamoTable, final String source) {
    this.pipelineId = pipelineId;
    this.runId = runId;
    this.reportBucket = reportBucket;
    this.snsArn = snsArn;
    this.pipelineDynamoTable = pipelineDynamoTable;
    this.source = source;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public String getRunId() {
    return runId;
  }

  public String getReportBucket() {
    return reportBucket;
  }

  public String getSnsArn() {
    return snsArn;
  }

  public String getPipelineDynamoTable() {
    return pipelineDynamoTable;
  }

  public String getSource() {
    return source;
  }
}
