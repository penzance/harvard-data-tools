package edu.harvard.data.pipeline;

public class SnsNotificationPipelineObject extends AbstractPipelineObject {

  protected SnsNotificationPipelineObject(final DataConfig params, final String id) {
    super(params, id, "SnsAlarm");
    set("role", params.dataPipelineRole);
  }

  void setSubject(final String subject) {
    set("subject", subject);
  }

  void setMessage(final String message) {
    set("message", message);
  }

  void setTopicArn(final String topic) {
    set("topicArn", topic);
  }

}
