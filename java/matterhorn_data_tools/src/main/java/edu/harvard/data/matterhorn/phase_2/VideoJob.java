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
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.matterhorn.MatterhornDataConfig;
import edu.harvard.data.matterhorn.bindings.phase1.Phase1Video;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2Video;

public class VideoJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    new VideoJob(config, phase).runJob();
  }

  public VideoJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
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
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/video", outputDir + "/video");
    return job;
  }
}

class VideoFileMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public VideoFileMapper() {
    this.hadoopUtils = new HadoopUtilities();
  }

  @Override
  protected void setup(final Context context) {
    final Format formatName = Format.valueOf(context.getConfiguration().get("format"));
    this.format = new FormatLibrary().getFormat(formatName);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    final Phase1Video video = new Phase1Video(format, parser.getRecords().get(0));
    context.write(new Text(video.getId()), hadoopUtils.convertToText(video, format));
  }
}

class VideoFileReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public VideoFileReducer() {
    this.hadoopUtils = new HadoopUtilities();
  }

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
      final Phase1Video v = new Phase1Video(format, parser.getRecords().get(0));

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
    context.write(hadoopUtils.convertToText(video, format), NullWritable.get());
  }
}
