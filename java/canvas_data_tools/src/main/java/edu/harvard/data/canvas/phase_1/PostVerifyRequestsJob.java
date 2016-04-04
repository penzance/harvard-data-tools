package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.canvas.bindings.phase1.Phase1Requests;

public class PostVerifyRequestsJob extends HadoopJob {

  private final String verifyDir;

  public PostVerifyRequestsJob(final Configuration hadoopConf, final AwsUtils aws,
      final URI hdfsService, final String dataDir, final String verifyDir) {
    super(hadoopConf, aws, hdfsService, dataDir, null);
    this.verifyDir = verifyDir;
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "postverify-requests");
    job.setMapperClass(PostVerifyRequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", null);
    addToCache(job, verifyDir + "/updated/requests");
    return job;
  }

}

class PostVerifyRequestMapper extends Mapper<Object, Text, Text, LongWritable> {

  private final Map<String, String> interestingRequests;
  private final TableFormat format;

  public PostVerifyRequestMapper() {
    this.interestingRequests = new HashMap<String, String>();
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (FSDataInputStream in = fs.open(path);) {
        String line = in.readLine();
        while (line != null) {
          final String[] parts = line.split("\t");
          interestingRequests.put(parts[0], parts[1]);
          line = in.readLine();
        }
      }
    }
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
