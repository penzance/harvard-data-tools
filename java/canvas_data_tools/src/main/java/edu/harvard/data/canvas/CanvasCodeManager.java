package edu.harvard.data.canvas;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.canvas.identity.CanvasIdentityHadoopManager;
import edu.harvard.data.canvas.phase_1.Phase1PostVerifier;
import edu.harvard.data.canvas.phase_1.Phase1PreVerifier;
import edu.harvard.data.canvas.phase_2.AdminRequestJob;
import edu.harvard.data.canvas.phase_2.RequestJob;
import edu.harvard.data.canvas.phase_3.SessionsJob;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.IdentityMapHadoopJob;

@SuppressWarnings("rawtypes")
public class CanvasCodeManager extends GeneratedCodeManager {

  private final CanvasIdentityHadoopManager identity;

  public CanvasCodeManager() {
    this.identity = new CanvasIdentityHadoopManager();
  }

  @Override
  public Class<? extends IdentityMapHadoopJob> getIdentityMapHadoopJob() {
    return CanvasIdentityMapHadoopJob.class;
  }

  @Override
  public List<Class<? extends Mapper>> getIdentityMapperClasses() {
    return identity.getMapperClasses();
  }

  @Override
  public List<Class<? extends Mapper>> getIdentityScrubberClasses() {
    return identity.getScrubberClasses();
  }

  @Override
  public Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs() {
    addJob(RequestJob.class, 2);
    addJob(AdminRequestJob.class, 2);
    addJob(SessionsJob.class, 3);
    return jobs;
  }

  @Override
  public List<String> getIdentityTableNames() {
    return identity.getIdentityTableNames();
  }

  @Override
  public Class<?> getIdentityPreverifyJob() {
    return Phase1PreVerifier.class;
  }

  @Override
  public Class<?> getIdentityPostverifyJob() {
    return Phase1PostVerifier.class;
  }
}
