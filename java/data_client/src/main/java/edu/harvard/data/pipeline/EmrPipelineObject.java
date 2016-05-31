package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.harvard.data.DataConfigurationException;

public class EmrPipelineObject extends AbstractPipelineObject {

  protected EmrPipelineObject(final DataConfig params, final String id)
      throws DataConfigurationException {
    super(params, id, "EmrCluster");
    set("useOnDemandOnLastAttempt", "true");
    set("keyPair", params.keypair);
    set("releaseLabel", params.emrReleaseLabel);
    set("terminateAfter", params.emrTerminateAfter);
    set("subnetId", params.appSubnet);
    set("bootstrapAction", getBootstrapAction(params));
    set("masterInstanceType", params.emrMasterInstanceType);
    if (params.emrMasterBidPrice != null) {
      set("masterInstanceBidPrice", params.emrMasterBidPrice);
    }
    if (Integer.parseInt(params.emrCoreInstanceCount) > 0) {
      set("coreInstanceType", params.emrCoreInstanceType);
      set("coreInstanceCount", params.emrCoreInstanceCount);
      if (params.emrCoreBidPrice != null) {
        set("coreInstanceBidPrice", params.emrCoreBidPrice);
      }
    }
    if (Integer.parseInt(params.emrTaskInstanceCount) > 0) {
      set("taskInstanceType", params.emrTaskInstanceType);
      set("taskInstanceCount", params.emrTaskInstanceCount);
      if (params.emrTaskBidPrice != null) {
        set("taskInstanceBidPrice", params.emrTaskBidPrice);
      }
    }
  }

  void setSchedule(final SchedulePipelineObject schedule) {
    set("schedule", schedule);
  }

  private String getBootstrapAction(final DataConfig params) {
    final String codeLocation = params.codeBucket + "/" + params.gitTagOrBranch + "/"
        + params.lowercaseDatasource;
    final String bootstrapScript = "s3://" + params.codeBucket + "/" + params.gitTagOrBranch + "/bootstrap.sh";
    final List<String> bootstrapParams = new ArrayList<String>();
    bootstrapParams.add(bootstrapScript);
    bootstrapParams.add(params.dataSourceSchemaVersion);
    bootstrapParams.add(codeLocation);
    bootstrapParams.add(params.gitTagOrBranch);
    bootstrapParams.add(params.intermediateBucket);
    bootstrapParams.add(params.redshiftAccessKey);
    bootstrapParams.add(params.redshiftAccessSecret);
    bootstrapParams.add(params.codeBucket);
    bootstrapParams.add(params.lowercaseDatasource);
    return StringUtils.join(bootstrapParams, ",");
  }
}
