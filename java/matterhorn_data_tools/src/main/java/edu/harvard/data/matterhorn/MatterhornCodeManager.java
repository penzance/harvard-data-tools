package edu.harvard.data.matterhorn;

import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.matterhorn.identity.MatterhornIdentityHadoopManager;
import edu.harvard.data.matterhorn.phase_1.Phase1PostVerifier;
import edu.harvard.data.matterhorn.phase_1.Phase1PreVerifier;

public class MatterhornCodeManager implements GeneratedCodeManager {

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

}
