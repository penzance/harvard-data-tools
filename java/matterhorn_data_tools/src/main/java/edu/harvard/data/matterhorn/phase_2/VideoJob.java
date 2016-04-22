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
import edu.harvard.data.matterhorn.bindings.phase1.Phase1Video;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2Video;

class VideoJob extends HadoopJob {

  public VideoJob(final Configuration hadoopConf, final AwsUtils aws,
      final URI hdfsService, final String inputDir, final String outputDir) {
    super(hadoopConf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "video-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(VideoFileMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(VideoFileReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/video", outputDir + "/video");
    return job;
  }
}

class VideoFileMapper extends Mapper<Object, Text, Text, Text> {

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
    final Phase1Video video;
    try {
      video = new Phase1Video(format, parser.getRecords().get(0));
    } catch (final Throwable t) {
      throw new RuntimeException("Value: " + value.toString(), t);
    }
    context.write(new Text(video.getId()), HadoopJob.convertToText(video, format));
  }
}

class VideoFileReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
  }

  @Override
  public void reduce(final Text key, final Iterable<Text> values, final Context context)
      throws IOException, InterruptedException {
    final Phase2Video video = new Phase2Video();
    video.setId(key.toString());
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      final Phase1Video v;
      try {
        v = new Phase1Video(format, parser.getRecords().get(0));
      } catch (final Throwable t) {
        throw new RuntimeException("Value: " + value.toString(), t);
      }

      if (video.getCdn() == null && v.getCdn() != null) {
        video.setCdn(v.getCdn());
      }
      if (video.getSeries() == null && v.getSeries() != null) {
        video.setSeries(v.getSeries());
      }
      if (video.getCourse() == null && v.getCourse() != null) {
        video.setCourse(v.getCourse());
      }
      if (video.getType() == null && v.getType() != null) {
        video.setType(v.getType());
      }
      if (video.getTitle() == null && v.getTitle() != null) {
        video.setTitle(v.getTitle());
      }
      if (video.getYear() == null && v.getYear() != null) {
        video.setYear(v.getYear());
      }
      if (video.getTerm() == null && v.getTerm() != null) {
        video.setTerm(v.getTerm());
      }
    }
    context.write(HadoopJob.convertToText(video, format), NullWritable.get());
  }
}
