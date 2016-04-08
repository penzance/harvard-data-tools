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

class EventTypeJob extends HadoopJob {

  public EventTypeJob(final Configuration hadoopConf, final AwsUtils aws,
      final URI hdfsService, final String inputDir, final String outputDir) {
    super(hadoopConf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "event-type-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(EventTypeMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(SessionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/event", outputDir + "/event_types");
    return job;
  }
}

class EventTypeMapper extends Mapper<Object, Text, Text, NullWritable> {

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
    context.write(new Text(event.getType()), NullWritable.get());
  }
}

class EventTypeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  @Override
  public void reduce(final Text key, final Iterable<NullWritable> values, final Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
