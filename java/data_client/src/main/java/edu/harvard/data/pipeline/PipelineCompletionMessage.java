package edu.harvard.data.pipeline;

public class PipelineCompletionMessage {
  private final String pipelineId;
  private final String reportBucket;
  private final String snsArn;
  private final String pipelineDynamoTable;

  public PipelineCompletionMessage(final String pipelineId, final String reportBucket,
      final String snsArn, final String pipelineDynamoTable) {
    this.pipelineId = pipelineId;
    this.reportBucket = reportBucket;
    this.snsArn = snsArn;
    this.pipelineDynamoTable = pipelineDynamoTable;
  }

  public String getPipelineId() {
    return pipelineId;
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
}
