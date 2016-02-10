package edu.harvard.data.hadoop.requests;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.FormatLibrary;
import edu.harvard.data.client.FormatLibrary.Format;
import edu.harvard.data.client.TableFormat;
import edu.harvard.data.client.canvas.phase0.Requests;

public class RequestPerFileJob extends HadoopJob {

  public RequestPerFileJob(final Configuration conf, final AwsUtils aws, final URI hdfsService,
      final String inputDir, final String outputDir) {
    super(conf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(conf, "requests-files-hadoop");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapperClass(RequestFileMapper.class);
    job.setReducerClass(RequestFileReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests_per_file");
    return job;
  }
}

class RequestFileMapper extends Mapper<Object, Text, Text, LongWritable> {

  private final TableFormat format;

  public RequestFileMapper() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Requests request = new Requests(format, csvRecord);

      final String url = request.getUrl().toLowerCase();
      if (url.contains("/files/")) {
        String fileKey = url;
        if (fileKey.contains("?")) {
          fileKey = fileKey.substring(0,  url.indexOf("?"));
          context.write(new Text(fileKey), new LongWritable(1));
        }
      }
    }
  }
}

class RequestFileReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  public void reduce(final Text key, final Iterable<LongWritable> values,
      final Context context)
          throws IOException, InterruptedException {
    long sum = 0;
    for (final LongWritable count : values) {
      sum += count.get();
    }
    context.write(key, new LongWritable(sum));
  }
}
