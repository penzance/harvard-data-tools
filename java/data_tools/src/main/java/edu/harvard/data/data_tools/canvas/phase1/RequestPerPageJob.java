package edu.harvard.data.data_tools.canvas.phase1;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import edu.harvard.data.client.canvas.phase1.Phase1RequestsPerPage;
import edu.harvard.data.data_tools.HadoopJob;

public class RequestPerPageJob extends HadoopJob {

  public RequestPerPageJob(final Configuration conf, final AwsUtils aws, final URI hdfsService,
      final String inputDir, final String outputDir) {
    super(conf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(conf, "requests-pages-hadoop");

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(RequestPageMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setReducerClass(RequestPageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);

    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests_per_page");
    return job;
  }
}

class RequestPageMapper extends Mapper<Object, Text, Text, LongWritable> {

  private final TableFormat format;

  public RequestPageMapper() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Requests request = new Requests(format, csvRecord);

      final String url = request.getUrl().toLowerCase();
      if (url.contains("/pages/")) {
        String fileKey = url;
        if (fileKey.contains("?")) {
          fileKey = fileKey.substring(0, url.indexOf("?"));
          context.write(new Text(fileKey), new LongWritable(1));
        }
      }
    }
  }
}

class RequestPageReducer extends Reducer<Text, LongWritable, Text, NullWritable> {

  private final TableFormat format;

  public RequestPageReducer() {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void reduce(final Text key, final Iterable<LongWritable> values, final Context context)
      throws IOException, InterruptedException {
    long sum = 0;
    for (final LongWritable count : values) {
      sum += count.get();
    }
    final Phase1RequestsPerPage record = new Phase1RequestsPerPage(key.toString(), sum);
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    final Text csvText = new Text(writer.toString().trim());
    context.write(csvText, NullWritable.get());
  }
}
