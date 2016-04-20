package edu.harvard.data.matterhorn.phase_2;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.matterhorn.bindings.phase1.Phase1Event;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2Session;

class SessionJob extends HadoopJob {

  public SessionJob(final Configuration hadoopConf, final AwsUtils aws,
      final URI hdfsService, final String inputDir, final String outputDir) {
    super(hadoopConf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "session-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(SessionMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(SessionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/event", outputDir + "/session");
    return job;
  }
}

class SessionMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    final Phase1Event event = new Phase1Event(format, parser.getRecords().get(0));
    context.write(new Text(event.getSessionId()), HadoopJob.convertToText(event, format));
  }
}

class SessionReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
  }

  @Override
  public void reduce(final Text key, final Iterable<Text> values, final Context context)
      throws IOException, InterruptedException {
    long latest = Long.MIN_VALUE;
    long earliest = Long.MAX_VALUE;
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      final Phase1Event event = new Phase1Event(format, parser.getRecords().get(0));
      final long time = event.getCreated().getTime();
      if (time > latest) {
        latest = time;
      }
      if (time < earliest) {
        earliest = time;
      }
    }
    final Phase2Session session = new Phase2Session();
    session.setSessionId(key.toString());
    session.setDuration(latest - earliest);
    context.write(HadoopJob.convertToText(session, format), NullWritable.get());
  }
}
