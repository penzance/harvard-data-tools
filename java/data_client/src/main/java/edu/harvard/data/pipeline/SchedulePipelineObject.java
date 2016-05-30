package edu.harvard.data.pipeline;

public class SchedulePipelineObject extends AbstractPipelineObject {

  protected SchedulePipelineObject(final DataConfig params, final String id) {
    super(params, id, "Schedule");
    set("occurrences", "1");
    set("startAt", "FIRST_ACTIVATION_DATE_TIME");
    set("period", "1 Day");
  }

}
