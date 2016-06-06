package edu.harvard.data.pipeline;

public class PipelineCompletionMessage {
  private final String pipelineId;
  private final String reportBucket;
  private final String snsArn;

  public PipelineCompletionMessage(final String pipelineId, final String reportBucket, final String snsArn) {
    this.pipelineId = pipelineId;
    this.reportBucket = reportBucket;
    this.snsArn = snsArn;
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

}
