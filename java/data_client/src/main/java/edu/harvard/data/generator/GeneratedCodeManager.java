package edu.harvard.data.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.identity.IdentityScrubHadoopJob;
import edu.harvard.data.identity.IdentityScrubber;

public abstract class GeneratedCodeManager {

  protected final Map<Integer, List<Class<? extends HadoopJob>>> jobs;

  protected GeneratedCodeManager() {
    jobs = new HashMap<Integer, List<Class<? extends HadoopJob>>>();
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

  public abstract Class<? extends IdentityMapHadoopJob> getIdentityMapHadoopJob();

  public abstract Class<? extends IdentityScrubHadoopJob> getIdentityScrubHadoopJob();

  public abstract Class<?> getIdentityPreverifyJob();

  public abstract Class<?> getIdentityPostverifyJob();

  public abstract Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs();

}
