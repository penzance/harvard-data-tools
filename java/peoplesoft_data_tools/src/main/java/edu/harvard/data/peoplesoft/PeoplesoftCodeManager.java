package edu.harvard.data.peoplesoft;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.Phase0;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityScrubber;
import edu.harvard.data.peoplesoft.PeoplesoftDataConfig;
import edu.harvard.data.peoplesoft.identity.PeoplesoftIdentityHadoopManager;

public class PeoplesoftCodeManager extends CodeManager {

  private final PeoplesoftIdentityHadoopManager identity;

  public PeoplesoftCodeManager() {
    this.identity = new PeoplesoftIdentityHadoopManager();
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
    return jobs;
  }

  @Override
  public DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException {
    return PeoplesoftDataConfig.parseInputFiles(PeoplesoftDataConfig.class, configPathString,
        verify);
  }

  @Override
  public Phase0 getPhase0(final String configPathString, final String datasetId, final String runId,
      final ExecutorService exec) throws IOException, DataConfigurationException {
    final PeoplesoftDataConfig config = PeoplesoftDataConfig
        .parseInputFiles(PeoplesoftDataConfig.class, configPathString, false);
    return new PeoplesoftPhase0(config, runId, exec);
  }

}
