package edu.harvard.data.canvas.phase_2;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.TableFormat;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.bindings.phase1.Phase1DiscussionEntryDim;
import edu.harvard.data.canvas.bindings.phase2.Phase2DiscussionEntryDim;

public class DiscussionEntryDimFullTextJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, true);
    new DiscussionEntryDimPass1(config, phase).runJob();
    new DiscussionEntryDimPass2(config, phase).runJob();
  }

}

class DiscussionEntryDimPass1 extends HadoopJob {

  public DiscussionEntryDimPass1(final DataConfig config, final int phase)
      throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "discussion-dim-1-hadoop");
    job.setMapperClass(DiscussionEntryDimPass1Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/discussion_entry_dim",
        outputDir + "/discussion_entry_dim");
    return job;
  }
}

class DiscussionEntryDimPass2 extends HadoopJob {

  public DiscussionEntryDimPass2(final DataConfig config, final int phase)
      throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "discussion-dim-2-hadoop");
    job.setMapperClass(DiscussionEntryDimPass2Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/discussion_entry_dim",
        outputDir + "/full_text/discussion_entry_dim");
    return job;
  }
}

class DiscussionEntryDimPass1Mapper extends Mapper<Object, Text, Text, NullWritable> {

  private TableFormat format;
  private HadoopUtilities hadoopUtils;

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
    this.hadoopUtils = new HadoopUtilities();
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase1DiscussionEntryDim in = new Phase1DiscussionEntryDim(format, csvRecord);
      final Phase2DiscussionEntryDim out = new Phase2DiscussionEntryDim(in);
      if (in.getMessage() != null) {
        final int length = in.getMessage().length();
        out.setMessageLength(length);
        out.setMessageTruncated(length > 256);
      }
      final Text csvText = hadoopUtils.convertToText(out, format);
      context.write(csvText, NullWritable.get());
    }
  }
}

class DiscussionEntryDimPass2Mapper extends Mapper<Object, Text, LongWritable, Text> {

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
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase1DiscussionEntryDim in = new Phase1DiscussionEntryDim(format, csvRecord);
      context.write(new LongWritable(in.getId()), new Text(in.getMessage()));
    }
  }
}
