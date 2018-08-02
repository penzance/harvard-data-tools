package edu.harvard.data.canvasrest;

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
import edu.harvard.data.canvasrest.CanvasrestDataConfig;
import edu.harvard.data.canvasrest.identity.CanvasrestIdentityHadoopManager;

public class CanvasrestCodeManager extends CodeManager {

  private final CanvasrestIdentityHadoopManager identity;

  public CanvasrestCodeManager() {
    this.identity = new CanvasrestIdentityHadoopManager();
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
    return CanvasrestDataConfig.parseInputFiles(CanvasrestDataConfig.class, configPathString,
        verify);
  }

  @Override
  public Phase0 getPhase0(final String configPathString, final String datasetId, final String runId,
      final ExecutorService exec) throws IOException, DataConfigurationException {
    final CanvasrestDataConfig config = CanvasrestDataConfig
        .parseInputFiles(CanvasrestDataConfig.class, configPathString, false);
    return new CanvasrestPhase0(config, runId, exec);
  }

}
