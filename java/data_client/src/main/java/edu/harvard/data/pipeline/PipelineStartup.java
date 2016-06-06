package edu.harvard.data.pipeline;

import java.util.Date;

// Run by the StartupActivity step
public class PipelineStartup {

  public static void main(final String[] args) {
    final String pipelineId = args[0];
    final String dynamoTable = args[1];
    // TODO: Check args.

    // TODO: Check for table existence
    PipelineExecutionRecord.init(dynamoTable);

    final PipelineExecutionRecord record = PipelineExecutionRecord.find(pipelineId);
    // TODO: Check for missing record

    record.setPipelineStart(new Date());
    record.save();
  }

}
