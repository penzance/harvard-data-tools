package edu.harvard.data.canvas.phase_3;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.bindings.phase2.Phase2Requests;
import edu.harvard.data.canvas.bindings.phase3.Phase3Sessions;
import edu.harvard.data.canvas.phase_2.RequestJob;

public class SessionsJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString,
        true);
    new RequestJob(config, phase).runJob();
  }

  public SessionsJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "sessions-hadoop");

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(SessionsMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(SessionsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);

    hadoopUtils.setPaths(job, hdfsService, inputDir + "/requests", outputDir + "/sessions");
    return job;
  }
}

class SessionsMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public SessionsMapper() {
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
    final Phase2Requests request = new Phase2Requests(format, parser.getRecords().get(0));
    if (request.getSessionId() != null) {
      context.write(new Text(request.getSessionId()), hadoopUtils.recordToText(request, format));
    }
  }
}

class SessionsReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public SessionsReducer() {
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
    String userResearchId = null;
    Timestamp earliest = new Timestamp(Long.MAX_VALUE);
    Timestamp latest = new Timestamp(Long.MIN_VALUE);
    int requestCount = 0;
    final Set<String> urls = new HashSet<String>();
    final Set<Long> courses = new HashSet<Long>();
    final Set<Long> assignments = new HashSet<Long>();
    final Set<Long> conversations = new HashSet<Long>();
    final Set<Long> discussions = new HashSet<Long>();
    final Set<Long> quizzes = new HashSet<Long>();

    final Map<String, Integer> ips = new HashMap<String, Integer>();
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      final Phase2Requests request = new Phase2Requests(format, parser.getRecords().get(0));
      requestCount++;
      if (request.getUrl() != null) {
        urls.add(request.getUrl());
      }
      if (request.getCourseId() != null) {
        courses.add(request.getCourseId());
      }
      if (request.getAssignmentId() != null) {
        assignments.add(request.getAssignmentId());
      }
      if (request.getConversationId() != null) {
        conversations.add(request.getConversationId());
      }
      if (request.getDiscussionId() != null) {
        discussions.add(request.getDiscussionId());
      }
      if (request.getQuizId() != null) {
        quizzes.add(request.getQuizId());
      }
      if (userResearchId == null) {
        userResearchId = request.getUserIdResearchUuid();
      }
      if (request.getTimestamp() != null && request.getTimestamp().after(latest)) {
        latest = request.getTimestamp();
      }
      if (request.getTimestamp() != null && request.getTimestamp().before(earliest)) {
        earliest = request.getTimestamp();
      }
      final String ip = request.getRemoteIp();
      if (ip != null) {
        if (!ips.containsKey(ip)) {
          ips.put(ip, 0);
        }
        ips.put(ip, ips.get(ip) + 1);
      }
    }

    final Phase3Sessions session = new Phase3Sessions();
    session.setSessionId(key.toString());

    session.setUserResearchId(userResearchId);
    session.setRequestCount(requestCount);
    session.setUniqueAssignments(assignments.size());
    session.setUniqueConversations(conversations.size());
    session.setUniqueCourses(courses.size());
    session.setUniqueDiscussions(discussions.size());
    session.setUniqueQuizzes(quizzes.size());
    session.setUniqueUrls(urls.size());

    session.setUniqueIps(ips.size());
    if (ips.size() > 1) {
      String mainIp = null;
      int maxUsages = -1;
      for (final String ip : ips.keySet()) {
        if (ips.get(ip) > maxUsages) {
          mainIp = ip;
          maxUsages = ips.get(ip);
        }
      }
      session.setMainIp(mainIp);
    }

    session.setStartTime(earliest);
    session.setEndTime(latest);
    session.setDurationMs(latest.getTime() - earliest.getTime());

    context.write(hadoopUtils.convertToText(session, format), NullWritable.get());
  }
}
