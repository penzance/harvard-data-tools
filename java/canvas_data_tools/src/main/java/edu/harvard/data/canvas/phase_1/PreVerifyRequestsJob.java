package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mortbay.log.Log;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
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
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(PreVerifyRequestReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests");
    return job;
  }

}

class PreVerifyRequestMapper extends Mapper<Object, Text, LongWritable, Text> {

  private final TableFormat format;
  private final HashSet<Long> interestingIds;

  public PreVerifyRequestMapper() throws IOException, DataConfigurationException {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
    this.interestingIds = new HashSet<Long>();
    interestingIds.add(-377893982974389669L);
    interestingIds.add(-315678870418912273L);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase0Requests request = new Phase0Requests(format, csvRecord);
      if (request.getUserId() != null && interestingIds.contains(request.getUserId())) {
        context.write(new LongWritable(request.getUserId()), new Text(request.getId()));
      }
    }
  }
}

class PreVerifyRequestReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

  private MultipleOutputs<Text, NullWritable> outputs;

  @Override
  public void setup(final Context context) {
    outputs = new MultipleOutputs<Text, NullWritable>(context);
  }

  @Override
  public void reduce(final LongWritable canvasDataId, final Iterable<Text> values,
      final Context context) throws IOException, InterruptedException {
    final String outputName = "request_preverify" + canvasDataId.toString().replaceAll("-", "_");
    Log.info("Writing output to file " + outputName);
    for (final Text value : values) {
      outputs.write(outputName, value, NullWritable.get());
    }
  }
}
