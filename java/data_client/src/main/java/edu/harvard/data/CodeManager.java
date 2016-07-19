package edu.harvard.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityScrubber;

public abstract class CodeManager {

  protected final Map<Integer, List<Class<? extends HadoopJob>>> jobs;

  protected CodeManager() {
    jobs = new HashMap<Integer, List<Class<? extends HadoopJob>>>();
  }

  @SuppressWarnings("unchecked")
  public static CodeManager getCodeManager(final String codeManagerClassName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final Class<? extends CodeManager> codeManagerClass = (Class<? extends CodeManager>) Class
        .forName(codeManagerClassName);
    return codeManagerClass.newInstance();
  }

  protected void addJob(final Class<? extends HadoopJob> job, final int phase) {
    if (!jobs.containsKey(phase)) {
      jobs.put(phase, new ArrayList<Class<? extends HadoopJob>>());
    }
    jobs.get(phase).add(job);
  }

  // Table name to class
  public abstract Map<String, Class<? extends Mapper<Object, Text, ?, HadoopIdentityKey>>> getIdentityMapperClasses();

  public abstract Map<String, Class<? extends IdentityScrubber<?>>> getIdentityScrubberClasses();

  public abstract List<String> getIdentityTableNames();

  public abstract Class<?> getIdentityPreverifyJob();

  public abstract Class<?> getIdentityPostverifyJob();

  public abstract Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs();

  public abstract DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException;

  public abstract Phase0 getPhase0(final String configPathString, final String datasetId,
      final String runId, final ExecutorService exec) throws IOException, DataConfigurationException;
}
