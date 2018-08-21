package edu.harvard.data;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.data.FormatLibrary.Format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class HadoopJob {
  private static final Logger log = LogManager.getLogger();

  protected Configuration hadoopConf;
  protected HadoopUtilities hadoopUtils;
  protected final URI hdfsService;
  protected List<URI> cacheFiles;
  protected final int phase;
  protected final DataConfig config;

  public HadoopJob(final DataConfig config, final int phase) throws DataConfigurationException {
    this.phase = phase;
    this.config = config;
    this.hadoopUtils = new HadoopUtilities();
    this.cacheFiles = new ArrayList<URI>();
    try {
      this.hdfsService = new URI("hdfs///");
    } catch (final URISyntaxException e) {
      throw new DataConfigurationException(e);
    }
    this.hadoopConf = new Configuration();
    final TableFormat format = new FormatLibrary().getFormat( Format.fromLabel( Format.DecompressedInternal.getLabel() ) );
    log.info( "pipeline format: " + config.getPipelineFormat() );
    log.info( "format: " + Format.fromLabel( Format.DecompressedInternal.getLabel() ));
    hadoopConf.set("format", format.getFormat().toString() );
    hadoopConf.set("config", config.getPaths());
  }

  public void runJob() throws IOException, DataConfigurationException {
    Job job;
    try {
      job = getJob();
    } catch (final NoInputDataException e) {
      // If there's no input data we don't run the job, but don't fail since we
      // may be running against a subset of the data.
      log.info(e.getMessage());
      return;
    }
    for (final URI file : cacheFiles) {
      job.addCacheFile(file);
    }
    job.setJarByClass(HadoopJob.class);
    try {
      log.info("Submitted job " + job.getJobName());
      //      job.submit();
      job.waitForCompletion(true);
      log.info("Job complete: " + job.getJobName());
    } catch (final ClassNotFoundException e) {
      throw new DataConfigurationException(e);
    } catch (final InterruptedException e) {
      log.error("Job submission interrupted", e);
    }
  }

  public void addCacheFile(final URI file) {
    this.cacheFiles.add(file);
  }

  public abstract Job getJob() throws IOException, NoInputDataException;

}
