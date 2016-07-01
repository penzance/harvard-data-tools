package edu.harvard.data.matterhorn;

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
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.TableFormat;
import edu.harvard.data.matterhorn.bindings.phase1.Phase1UserAgent;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2UserAgent;

public class UserAgentJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    new UserAgentJob(config, phase).runJob();
  }

  public UserAgentJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "useragent-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(UserAgentMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(UserAgentReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/user_agent", outputDir + "/user_agent");
    return job;
  }
}

class UserAgentMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public UserAgentMapper() {
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
    final Phase1UserAgent ua = new Phase1UserAgent(format, parser.getRecords().get(0));
    context.write(new Text(ua.getUseragent()), hadoopUtils.convertToText(ua, format));
  }
}

class UserAgentReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public UserAgentReducer() {
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
    final Phase2UserAgent ua = new Phase2UserAgent();
    ua.setUseragent(key.toString());
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      final Phase1UserAgent u = new Phase1UserAgent(format, parser.getRecords().get(0));
      if (ua.getBuild() == null && u.getBuild() != null) {
        ua.setBuild(u.getBuild());
      }
      if (ua.getDevice() == null && u.getDevice() != null) {
        ua.setDevice(u.getDevice());
      }
      if (ua.getMajor() == null && u.getMajor() != null) {
        ua.setMajor(u.getMajor());
      }
      if (ua.getMinor() == null && u.getMinor() != null) {
        ua.setMinor(u.getMinor());
      }
      if (ua.getName() == null && u.getName() != null) {
        ua.setName(u.getName());
      }
      if (ua.getOs() == null && u.getOs() != null) {
        ua.setOs(u.getOs());
      }
      if (ua.getOsMajor() == null && u.getOsMajor() != null) {
        ua.setOsMajor(u.getOsMajor());
      }
      if (ua.getOsMinor() == null && u.getOsMinor() != null) {
        ua.setOsMinor(u.getOsMinor());
      }
      if (ua.getOsName() == null && u.getOsName() != null) {
        ua.setOsName(u.getOsName());
      }
      if (ua.getPatch() == null && u.getPatch() != null) {
        ua.setPatch(u.getPatch());
      }
    }
    context.write(hadoopUtils.convertToText(ua, format), NullWritable.get());
  }
}
