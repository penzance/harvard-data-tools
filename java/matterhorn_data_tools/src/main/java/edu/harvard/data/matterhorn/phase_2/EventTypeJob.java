package edu.harvard.data.matterhorn.phase_2;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.matterhorn.MatterhornDataConfig;
import edu.harvard.data.matterhorn.bindings.phase1.Phase1Event;

class EventTypeJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    new EventTypeJob(config, phase).runJob();
  }

  public EventTypeJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "event-type-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(EventTypeMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(EventTypeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/event", outputDir + "/event_types");
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
