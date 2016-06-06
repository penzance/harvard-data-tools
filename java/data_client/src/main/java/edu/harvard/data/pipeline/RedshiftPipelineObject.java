package edu.harvard.data.pipeline;

import com.amazonaws.services.datapipeline.model.PipelineObject;

public class RedshiftPipelineObject extends AbstractPipelineObject {

  protected RedshiftPipelineObject(final DataConfig params, final String id) {
    super(params, id, "RedshiftDatabase");
    final PipelineObject db = new PipelineObject();
    set("clusterId", params.redshiftCluster);
    set("username", params.redshiftUserName);
    set("*password", params.redshiftPassword);
    set("databaseName", params.redshiftDatabase);
    db.setFields(fields);
  }

}
