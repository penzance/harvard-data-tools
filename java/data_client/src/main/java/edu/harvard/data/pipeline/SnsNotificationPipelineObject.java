package edu.harvard.data.pipeline;

public class SnsNotificationPipelineObject extends AbstractPipelineObject {

  protected SnsNotificationPipelineObject(final DataConfig params, final String id,
      final String subject, final String msg, final String topicArn) {
    super(params, id, "SnsAlarm");
    set("role", params.dataPipelineRole);
    set("subject", subject);
    set("message", msg);
    set("topicArn", topicArn);
  }

}
