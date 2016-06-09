package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import edu.harvard.data.DataConfig;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.generator.GeneratedCodeManager;

public class Phase2PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final GeneratedCodeManager codeManager;

  public Phase2PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final GeneratedCodeManager codeManager) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.codeManager = codeManager;
    this.config = pipeline.getConfig();
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    final Map<Integer, List<Class<? extends HadoopJob>>> jobsByPhase = codeManager
        .getHadoopProcessingJobs();
    final List<Integer> phases = new ArrayList<Integer>(jobsByPhase.keySet());
    if (phases.isEmpty()) {
      previousStep = skipPhase2(previousStep);
    } else {
      Collections.sort(phases);
      for (final Integer phase : phases) {
        previousStep = populatePhase(previousStep, phase, jobsByPhase.get(phase));
      }
    }

    return previousStep;
  }

  private PipelineObjectBase populatePhase(final PipelineObjectBase previousStep, final int phase,
      final List<Class<? extends HadoopJob>> jobs) {
    final PipelineObjectBase moveUnmodified = moveUnmodifiedFiles(phase);
    for (final Class<? extends HadoopJob> job : jobs) {
      final PipelineObjectBase hadoopStep = runHadoopJob(job, previousStep, phase);
      moveUnmodified.addDependency(hadoopStep);
    }
    return moveUnmodified;
  }

  private PipelineObjectBase runHadoopJob(final Class<?> cls,
      final PipelineObjectBase previousStep, final int phase) {
    final String id = "Phase" + phase + "Hadoop" + cls.getSimpleName();
    final List<String> args = new ArrayList<String>();
    args.add(config.paths);
    args.add("" + phase);
    final PipelineObjectBase job = factory.getEmrActivity(id, pipeline.getEmr(),
        cls, args);
    job.addDependency(previousStep);
    return job;
  }

  private PipelineObjectBase moveUnmodifiedFiles(final int phase) {
    final String cmd = config.getMoveUnmodifiedScript(phase);
    final String id = "Phase" + phase + "MoveUnmodifiedFiles";
    final PipelineObjectBase moveUnmodified = factory.getShellActivity(id, cmd, pipeline.getEmr());
    return moveUnmodified;
  }

  private PipelineObjectBase skipPhase2(final PipelineObjectBase previousStep) {
    final String cmd = "hadoop fs -mkdir " + config.getHdfsDir(2) + "; hadoop fs -mv hdfs://"
        + config.getHdfsDir(1) + "/* hdfs://" + config.getHdfsDir(2) + "/";
    final PipelineObjectBase skip = factory.getShellActivity("SkipPhase2", cmd, pipeline.getEmr());
    skip.addDependency(previousStep);
    return skip;
  }

}
