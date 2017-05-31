package edu.harvard.data.canvas.phase_1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mortbay.log.Log;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.TableFormat;
import edu.harvard.data.canvas.bindings.phase1.Phase1Requests;

/**
 * Wrapper class around the {@link PostVerifyRequestMapper} that configures a
 * new Hadoop job to do post-verification.
 */
public class PostVerifyRequestsJob extends HadoopJob {

  public PostVerifyRequestsJob(final DataConfig config, final int phase)
      throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "postverify-requests");
    job.setMapperClass(PostVerifyRequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase);
    final String verifyDir = config.getVerifyHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/requests", null);
    hadoopUtils.addToCache(job, hdfsService, verifyDir + "/updated/requests");
    return job;
  }
}

/**
 * Request post-verification Hadoop job. This class reads in the set of
 * interesting requests that were saved by the {@link PreVerifyRequestsJob}
 * Hadoop mapper, and stores them in local memory. It then runs through all
 * requests, looking for request IDs that were flagged as interesting.
 *
 * For every interesting request, we stored the Canvas Data ID of the user
 * making the request, and then converted it to the research UUID stored in the
 * identity map (see {@link Phase1PostVerifier#updateInterestingTables()}). We
 * can then check that the research UUID stored in the record read in via
 * Hadoop's map mechanism matches what we expected to see according to our
 * pre-calculated set of identifiers.
 */
class PostVerifyRequestMapper extends Mapper<Object, Text, Text, LongWritable> {

  private final Map<String, String> interestingRequests;
  private TableFormat format;

  public PostVerifyRequestMapper() {
    this.interestingRequests = new HashMap<String, String>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (FSDataInputStream fsin = fs.open(path);
          BufferedReader in = new BufferedReader(new InputStreamReader(fsin))) {
        String line = in.readLine();
        while (line != null) {
          final String[] parts = line.split("\t");
          interestingRequests.put(parts[0], parts[1]);
          line = in.readLine();
        }
      }
    }
    Log.info("Read " + interestingRequests.size() + " interesting requests");
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase1Requests request = new Phase1Requests(format, csvRecord);
      if (interestingRequests.containsKey(request.getId())) {
        final String researchId = interestingRequests.get(request.getId());
        if (!request.getUserIdResearchUuid().equals(researchId)) {
          throw new RuntimeException("Validation error: Expected to find research ID " + researchId
              + ", but instead found " + request.getUserIdResearchUuid() + " in request "
              + request.getId());
        }
      }
    }
  }
}
