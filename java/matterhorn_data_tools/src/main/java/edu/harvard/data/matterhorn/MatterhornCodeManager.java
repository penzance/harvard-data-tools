package edu.harvard.data.matterhorn;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.matterhorn.identity.MatterhornIdentityHadoopManager;
import edu.harvard.data.matterhorn.phase_1.Phase1PostVerifier;
import edu.harvard.data.matterhorn.phase_1.Phase1PreVerifier;
import edu.harvard.data.matterhorn.phase_2.SessionJob;
import edu.harvard.data.matterhorn.phase_2.VideoJob;

public class MatterhornCodeManager extends GeneratedCodeManager {

  private final MatterhornIdentityHadoopManager identity;

  public MatterhornCodeManager() {
    this.identity = new MatterhornIdentityHadoopManager();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Class<? extends Mapper>> getIdentityMapperClasses() {
    return identity.getMapperClasses();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Class<? extends Mapper>> getIdentityScrubberClasses() {
    return identity.getScrubberClasses();
  }

  @Override
  public List<String> getIdentityTableNames() {
    return identity.getIdentityTableNames();
  }

  @Override
  public Class<? extends IdentityMapHadoopJob> getIdentityMapHadoopJob() {
    return MatterhornIdentityMapHadoopJob.class;
  }

  @Override
  public Class<?> getIdentityPreverifyJob() {
    return Phase1PreVerifier.class;
  }

  @Override
  public Class<?> getIdentityPostverifyJob() {
    return Phase1PostVerifier.class;
  }

  @Override
  public Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs() {
    addJob(SessionJob.class, 2);
    addJob(VideoJob.class, 2);
    return jobs;
  }

}
