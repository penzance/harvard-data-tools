package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.CodeManager;

public class Phase3PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;
  private final CodeManager codeManager;
  private final InputTableIndex dataIndex;

  public Phase3PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final CodeManager codeManager, final String runId, final InputTableIndex dataIndex) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.codeManager = codeManager;
    this.dataIndex = dataIndex;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(runId));
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.getRedshiftStagingDir());
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    previousStep = copyDataToS3(previousStep);
    // Update Redshift schema
    if (!dataIndex.containsTable("requests") || dataIndex.getPartial().get("requests")) {
      // XXX This means we're not dealing with a megadump, which needs some manual
      // intervention to load the data to Redshift. Need to handle this case
      // specially.
      previousStep = loadData(previousStep);
    }
    // Delete data from working bucket
    // Move data from incoming bucket to archive
    
    // Transform Data
    previousStep = transformData(previousStep);

    return previousStep;
  }

  private PipelineObjectBase copyDataToS3(final PipelineObjectBase previousStep) {
    final PipelineObjectBase copy = factory.getS3DistCpActivity("CopyAllTablesToS3",
        config.getHdfsDir(getLastPhase()), redshiftStagingS3, pipeline.getEmr());
    copy.addDependency(previousStep);
    return copy;
  }
  
  private PipelineObjectBase transformData( final PipelineObjectBase previousStep ) {
	final S3ObjectId script = AwsUtils.key(workingDir, "code", config.getRapidConfigFile());
    final List<String> args = new ArrayList<String>();
    
    args.add( config.getRapidCodeDir() + config.getRapidRuntime() );
    args.add("--data-requests"); // args[0] in main class
    args.add("--runtime"); // args[1] in main class
    // <rapidCodeDir><rapidRuntime> --data-requests --runtime
    
	final PipelineObjectBase transform = factory.getPythonShellActivity("TransformDataProducts", script, 
		args, pipeline.getEmr());
	return transform;
  }

  private PipelineObjectBase loadData(final PipelineObjectBase previousStep) {
    final S3ObjectId script = AwsUtils.key(workingDir, "code", config.getRedshiftLoadScript());
    final PipelineObjectBase load = factory.getSqlScriptActivity("LoadAllTablesToRedshift", script,
        pipeline.getRedshift(), pipeline.getEmr());
    load.addDependency(previousStep);
    return load;
  }

  private int getLastPhase() {
	// Set minimum to 2
	// Currently, even if no Hadoop Processing jobs are added, the 
	// 'skipPhase2' step in Phase2Pipeline will still move files 
	// on HDFS from /phase_1 to /phase_2
	// This was originally set to 0, which would FAIL on step 'CopyAllTablesToS3'
	// when attempting to copy from /phase_0 hdfs files which have been moved to /phase_2
	int last = 2;
    for (final Integer i : codeManager.getHadoopProcessingJobs().keySet()) {
      if (i > last) {
        last = i;
      }
    }
    return last;
  }

}
