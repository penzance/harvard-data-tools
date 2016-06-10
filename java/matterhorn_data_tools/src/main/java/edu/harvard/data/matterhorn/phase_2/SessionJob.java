package edu.harvard.data.matterhorn.phase_2;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

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
import edu.harvard.data.matterhorn.bindings.phase1.Phase1Event;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2Session;

public class SessionJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    new SessionJob(config, phase).runJob();
  }

  public SessionJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
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
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/event", outputDir + "/session");
    return job;
  }
}

class SessionMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public SessionMapper() {
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
    final Phase1Event event = new Phase1Event(format, parser.getRecords().get(0));
    context.write(new Text(event.getSessionId()), hadoopUtils.convertToText(event, format));
  }
}

class SessionReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public SessionReducer() {
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
    long latest = Long.MIN_VALUE;
    long earliest = Long.MAX_VALUE;
    final Map<String, Integer> ips = new HashMap<String, Integer>();
    final Map<String, Integer> ids = new HashMap<String, Integer>();
    final Map<String, Integer> mpids = new HashMap<String, Integer>();
    int events = 0;
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      events++;
      final Phase1Event event = new Phase1Event(format, parser.getRecords().get(0));
      addItem(ips, event.getIp());
      addItem(ids, event.getHuidResearchUuid());
      addItem(mpids, event.getMpid());
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
    session.setEventCount(events);
    session.setIpCount(ips.size());
    session.setMainIp(getMainValue(ips));
    session.setUserCount(ids.size());
    session.setMainUser(getMainValue(ids));
    session.setVideoCount(mpids.size());
    session.setMainVideo(getMainValue(mpids));
    session.setEndTime(new Timestamp(latest));
    session.setStartTime(new Timestamp(earliest));
    session.setDuration(latest - earliest);
    context.write(hadoopUtils.convertToText(session, format), NullWritable.get());
  }

  private String getMainValue(final Map<String, Integer> ips) {
    int max = -1;
    String value = null;
    for (final String key : ips.keySet()) {
      if (ips.get(key) > max) {
        value = key;
        max = ips.get(key);
      }
    }
    return value;
  }

  private void addItem(final Map<String, Integer> map, final String key) {
    if (!map.containsKey(key)) {
      map.put(key, 0);
    }
  }
}
