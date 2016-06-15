package edu.harvard.data.canvas.phase_1;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;

class PreVerifyRequestsJob extends HadoopJob {

  public PreVerifyRequestsJob(final DataConfig config, final int phase) throws DataConfigurationException {
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

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0Requests request = new Phase0Requests(format, csvRecord);
      if (request.getUserId() != null && idByCanvasDataId.containsKey(request.getUserId())) {
        context.write(new Text(request.getId()), new LongWritable(request.getUserId()));
      }
    }
  }
}
