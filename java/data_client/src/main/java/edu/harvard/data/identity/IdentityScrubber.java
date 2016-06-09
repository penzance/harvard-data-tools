package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.generator.IdentityScrubberGenerator;
import edu.harvard.data.io.TableReader;

/**
 * Base class for the identity scrub mapper job. As the second part of the
 * identity phase, an instance of this map job is run for every identifying
 * table in the data set. It is recommended but not required that these
 * subclasses be generated; see {@link IdentityScrubberGenerator} for code that
 * generates the subclasses.
 * <p>
 * There is no corresponding reduce step for this map job; the output of the
 * mapper is written directly as the output of the identity phase.
 * <p>
 * The class contains a setup method that reads an existing identity map from
 * the Hadoop distributed cache to create an {@code Map} from the main
 * identifier type (type parameter T) to {@link IdentityMap}. It also contains a
 * map method that uses the subtype's {@link #populateRecord} method to copy all
 * non-identifier fields from the previous phase's version of the record.
 *
 * @param <T>
 *          the type of the dataset's main identifier.
 */
public abstract class IdentityScrubber<T> extends Mapper<Object, Text, Text, NullWritable> {
  private static final Logger log = LogManager.getLogger();

  @SuppressWarnings("rawtypes")
  protected Job getJob(final String tableName, final Class<? extends Mapper> cls,
      final String configString) throws IOException {
    try {
      final HadoopUtilities hadoopUtils = new HadoopUtilities();
      final DataConfig config = DataConfig.parseInputFiles(DataConfig.class, configString, true);
      final Configuration hadoopConfig = new Configuration();

      hadoopConfig.set("format", Format.DecompressedCanvasDataFlatFiles.toString());
      hadoopConfig.set("config", configString);
      final Job job = Job.getInstance(hadoopConfig, "canvas-" + tableName + "-scrubber");
      job.setJarByClass(IdentityScrubber.class);
      job.setMapperClass(cls);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setNumReduceTasks(0);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      final URI hdfsService = new URI("hdfs///");
      hadoopUtils.setPaths(job, hdfsService, config.getHdfsDir(0) + "/" + tableName,
          config.getHdfsDir(1) + "/" + tableName);
      for (final Path path : hadoopUtils.listHdfsFiles(hadoopConfig,
          new Path(config.getHdfsDir(1) + "/identity_map"))) {
        job.addCacheFile(path.toUri());
      }

      return job;
    } catch (final URISyntaxException | DataConfigurationException e) {
      throw new IOException(e);
    }
  }

  /**
   * Parse a {@link CSVRecord} object to create an instance of the
   * {@link DataTable} type that represents this table.
   *
   * @param csvRecord
   *          the data to be populated in the new object.
   * @return a parsed object containing the data from the csvRecord parameter.
   *         This {@code DataTable} object should represent the state of the
   *         record at the end of the previous phase (e.g. if the identity phase
   *         is running as Phase 1 of the data pipeline, the returned value
   *         should be of the correct type to represent the output of Phase 0).
   */
  protected abstract DataTable populateRecord(CSVRecord csvRecord);

  protected TableFormat format;
  protected Map<T, IdentityMap> identities;
  protected HadoopUtilities hadoopUtils;

  @Override
  @SuppressWarnings("unchecked")
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    this.hadoopUtils = new HadoopUtilities();
    this.format = hadoopUtils.getFormat(context);
    DataConfig config;
    try {
      config = hadoopUtils.getConfig(context);
    } catch (final DataConfigurationException e) {
      throw new IOException(e);
    }
    final IdentifierType mainIdentifier = config.mainIdentifier;
    this.identities = new HashMap<T, IdentityMap>();
    try (TableReader<IdentityMap> in = hadoopUtils.getHdfsTableReader(context, format,
        IdentityMap.class)) {
      for (final IdentityMap id : in) {
        identities.put((T) id.get(mainIdentifier), id);
      }
    }
    log.info("Completed setup for " + this);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final DataTable record = populateRecord(csvRecord);
      context.write(hadoopUtils.recordToText(record, format), NullWritable.get());
    }
  }
}
