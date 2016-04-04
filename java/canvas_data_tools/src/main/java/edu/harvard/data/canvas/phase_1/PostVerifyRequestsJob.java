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

  public PostVerifyRequestsJob(final Configuration hadoopConf, final AwsUtils aws,
      final URI hdfsService, final String dataDir, final String verifyDir) {
    super(hadoopConf, aws, hdfsService, dataDir, null);
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
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests");
    return job;
  }

}

class PostVerifyRequestMapper extends Mapper<Object, Text, Text, LongWritable> {

  private final Map<String, Long> interestingRequests;
  private final Map<Long, String> observedIds;
  private final TableFormat format;

  public PostVerifyRequestMapper() {
    this.interestingRequests = new HashMap<String, Long>();
    this.observedIds = new HashMap<Long, String>();
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    for (final URI uri : context.getCacheFiles()) {
      final Path path = new Path(uri.toString());
      try (FSDataInputStream in = fs.open(path);) {
        final String line = in.readLine();
        while (line != null) {
          final String[] parts = line.split("\t");
          interestingRequests.put(parts[0], Long.parseLong(parts[1]));
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
        final Long originalId = interestingRequests.get(request.getId());
        if (!observedIds.containsKey(originalId)) {
          observedIds.put(originalId, request.getUserIdResearchUuid());
        }
        if (!request.getUserIdResearchUuid().equals(observedIds.get(originalId))) {
          throw new RuntimeException("Validation error: Original user ID " + originalId
              + " maps to more than one research ID in the requests table");
        }
      }
    }
  }

}
