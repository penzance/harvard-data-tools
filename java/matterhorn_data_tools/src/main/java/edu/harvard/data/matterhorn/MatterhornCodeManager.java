package edu.harvard.data.matterhorn;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.identity.IdentityScrubHadoopJob;
import edu.harvard.data.identity.IdentityScrubber;
import edu.harvard.data.matterhorn.identity.MatterhornIdentityHadoopManager;

public class MatterhornCodeManager extends GeneratedCodeManager {

  private final MatterhornIdentityHadoopManager identity;

  public MatterhornCodeManager() {
    this.identity = new MatterhornIdentityHadoopManager();
  }

  @Override
  public Map<String, Class<? extends Mapper<Object, Text, ?, HadoopIdentityKey>>> getIdentityMapperClasses() {
    return identity.getMapperClasses();
  }

  @Override
  public Map<String, Class<? extends IdentityScrubber<?>>> getIdentityScrubberClasses() {
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
  public Class<? extends IdentityScrubHadoopJob> getIdentityScrubHadoopJob() {
    return MatterhornIdentityScrubHadoopJob.class;
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
    addJob(GeoIpJob.class, 2);
    addJob(VideoJob.class, 2);
    return jobs;
  }
}
