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

/**
 * The CodeManager defines a factory interface through which dataset-specific
 * extensions to the data client can provide custom implementations of classes
 * and interfaces. By working through this factory we can avoid having any
 * references to any particular data set in the main data-client code; all code
 * in the data-client project is written in terms of interfaces and abstract
 * classes, with concrete implementations provided at run time.
 */
public abstract class CodeManager {

  protected final Map<Integer, List<Class<? extends HadoopJob>>> jobs;

  protected CodeManager() {
    jobs = new HashMap<Integer, List<Class<? extends HadoopJob>>>();
  }

  /**
   * Initialize the code manager reflectively, using the class name passed in as
   * an argument. While this is not ideal, it allows us to defer until runtime
   * in creating the factory instance, which will be on the runtime class path
   * (as part of the dataset specific data client implementation).
   *
   * @param codeManagerClassName
   *          The fully-qualified name of the factory implementation class.
   * @return a CodeManager subtype that corresponds to the running data type.
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  @SuppressWarnings("unchecked")
  public static CodeManager getCodeManager(final String codeManagerClassName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final Class<? extends CodeManager> codeManagerClass = (Class<? extends CodeManager>) Class
        .forName(codeManagerClassName);
    return codeManagerClass.newInstance();
  }

  /**
   * Helper method to add a Hadoop job to an internal map from processing phase
   * to jobs. Registering a job ensures that it will be added to the pipeline
   * for the appropriate phase by the pipeline generator.
   *
   * @param job
   * @param phase
   */
  protected void addJob(final Class<? extends HadoopJob> job, final int phase) {
    if (!jobs.containsKey(phase)) {
      jobs.put(phase, new ArrayList<Class<? extends HadoopJob>>());
    }
    jobs.get(phase).add(job);
  }

  /**
   * Get the class types for all identity map Hadoop jobs to be executed during
   * the identity phase. These classes should represent generated code, for
   * which jobs will be created reflectively at run time.
   *
   * @return a map from table name to the appropriate identity mapper class.
   */
  public abstract Map<String, Class<? extends Mapper<Object, Text, ?, HadoopIdentityKey>>> getIdentityMapperClasses();

  /**
   * Get the class types for all identity scrub Hadoop jobs to be executed
   * during the identity phase.
   *
   * @return a map from table name to the appropriate generated identity
   *         scrubber class.
   */
  public abstract Map<String, Class<? extends IdentityScrubber<?>>> getIdentityScrubberClasses();

  /**
   * Get a list of tables that have to be processed during the identity phase.
   * These tables are identified by the identity schema specifications provided
   * to the code generator.
   *
   * @return a list of table names.
   */
  public abstract List<String> getIdentityTableNames();

  /**
   * Get a Java class containing a main method that will be executed before any
   * identity processing occurs during the identity phase. This class should
   * compute and store any relevant information that can be used later to verify
   * the correctness of the transformed tables.
   *
   * The pre-verify phase is strongly recommended, but is optional
   *
   * @return a Class object that contains an executable main method, or null if
   *         no pre-verification is to be performed.
   */
  public abstract Class<?> getIdentityPreverifyJob();

  /**
   * Get a Java class containing a main method that will be executed after all
   * identity processing has occurred during the identity phase. This class may
   * be used in conjunction with any data stored by a pre-verify job in order to
   * guarantee that the identity transformation process was performed correctly.
   *
   * The post-verify phase is strongly recommended, but is optional
   *
   * @return a Class object that contains an executable main method, or null if
   *         no post-verification is to be performed.
   */
  public abstract Class<?> getIdentityPostverifyJob();

  /**
   * Get a map of custom processing Hadoop job classes for this dataset, indexed
   * by execution phase. The jobs themselves will be created reflectively during
   * the appropriate pipeline phase.
   *
   * Subtypes of this class may use the protected {@code jobs} field to cache
   * and return this data, as well as the protected {@code addJob} method to
   * manage the map.
   *
   * @return a mapping from execution phase to a list of HadoopJob subtypes.
   */
  public abstract Map<Integer, List<Class<? extends HadoopJob>>> getHadoopProcessingJobs();

  /**
   * Get the dataset-specific subclass of the DataConfig class. This subclass
   * should handle any dataset-specific configuration settings.
   *
   * @param configPathString
   *          a standard pipe-delimited list of S3 locations where configuration
   *          files can be found. The files are formatted as standard Java
   *          properties files. Implementations of this method should not make
   *          any assumptions about the number or contents of the files.
   * @param verify
   *          whether or not the config files are expected to contain a complete
   *          set of configuration parameters. If this flag is set to true, an
   *          implementation should check that all settings have been assigned
   *          values.
   * @return a subclass of the DataConfig abstract class which can be used
   *         during a data processing run to access configuration settings.
   * @throws IOException
   * @throws DataConfigurationException
   */
  public abstract DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException;

  /**
   * Get an instance of the Phase0 abstract class that will download, normalize
   * and verify a data dump for this data type. The Phase0 implementation should
   * expect to run in a highly-concurrent environment (using the supplied
   * ExecutorService, which will have been configured with an appropriate number
   * of threads).
   *
   * @param configPathString
   * @param datasetId
   * @param runId
   * @param exec
   * @return an instance of a Phase0 subclass that will process a data dump for
   *         this data set.
   * @throws IOException
   * @throws DataConfigurationException
   */
  public abstract Phase0 getPhase0(final String configPathString, final String datasetId,
      final String runId, final ExecutorService exec)
          throws IOException, DataConfigurationException;
}
