package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.HadoopJob;
import edu.harvard.data.canvas.bindings.phase0.Phase0Requests;

class PreVerifyRequestsJob extends HadoopJob {

  public PreVerifyRequestsJob(final Configuration hadoopConf, final URI hdfsService,
      final String inputDir, final String outputDir) {
    super(hadoopConf, null, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "preverify-requests");
    job.setMapperClass(PreVerifyRequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests");
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
