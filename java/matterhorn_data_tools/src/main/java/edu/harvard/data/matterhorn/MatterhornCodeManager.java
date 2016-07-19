package edu.harvard.data.matterhorn;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.HadoopIdentityKey;
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
  public Class<?> getIdentityPreverifyJob() {
    return null;
  }

  @Override
  public Class<?> getIdentityPostverifyJob() {
    return null;
  }

  @Override
  public Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs() {
    addJob(GeoIpJob.class, 2);
    addJob(VideoJob.class, 2);
    return jobs;
  }

  @Override
  public DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException {
    return MatterhornDataConfig.parseInputFiles(MatterhornDataConfig.class, configPathString,
        verify);
  }
}
