package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.io.HdfsTableReader;

class PreVerifyRequestsJob extends HadoopJob {

  public PreVerifyRequestsJob(final DataConfig config, final int phase)
      throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "preverify-requests");
    job.setMapperClass(PreVerifyRequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getVerifyHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/requests", outputDir + "/requests");
    return job;
  }
}

class PreVerifyRequestMapper extends PreVerifyMapper {

  private static final Logger log = LogManager.getLogger();
  private final Map<Long, IdentityMap> interestingPeople;

  public PreVerifyRequestMapper() {
    super();
    this.interestingPeople = new HashMap<Long, IdentityMap>();
  }

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Path path = new Path(
        context.getConfiguration().get("/verify/phase_1/interesting_canvas_data_ids"));
    final FileSystem fs = FileSystem.get(context.getConfiguration());
    try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
        format, fs, path)) {
      for (final IdentityMap id : in) {
        interestingPeople.put((Long) id.get(IdentifierType.CanvasDataID), id);
      }
    }
    log.info("Finished setting up PreVerifyRequestMapper. Read " + interestingPeople.size()
    + " interesting people");
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0Requests request = new Phase0Requests(format, csvRecord);
      if (request.getUserId() != null && interestingPeople.containsKey(request.getUserId())) {
        System.err.println("Interesting request: " + csvRecord);
        context.write(new Text(request.getId()), new LongWritable(request.getUserId()));
      }
    }
  }
}
