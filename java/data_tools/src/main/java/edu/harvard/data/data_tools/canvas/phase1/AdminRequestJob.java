package edu.harvard.data.data_tools.canvas.phase1;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.FormatLibrary;
import edu.harvard.data.client.FormatLibrary.Format;
import edu.harvard.data.client.TableFormat;
import edu.harvard.data.client.canvas.phase0.Requests;
import edu.harvard.data.client.canvas.phase1.Phase1AdminRequests;
import edu.harvard.data.client.canvas.phase1.Phase1Requests;
import edu.harvard.data.data_tools.HadoopJob;

public class AdminRequestJob extends HadoopJob {

  public AdminRequestJob(final Configuration conf, final DataConfiguration dataConfig,
      final AwsUtils aws, final URI hdfsService, final String inputDir, final String outputDir) {
    super(conf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(conf, "admin-requests");
    job.setMapperClass(AdminRequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/admin_requests");
    return job;
  }
}

class AdminRequestMapper extends Mapper<Object, Text, Text, NullWritable> {

  private final TableFormat format;

  public AdminRequestMapper() throws IOException, DataConfigurationException {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase1Requests request = new Phase1Requests(new Requests(format, csvRecord));

      if (request.getUserId() != null && (request.getUserId() == -262295411484124942L
          || request.getUserId() == 134926641248969922L)) {
        final StringWriter writer = new StringWriter();
        try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
          printer.printRecord(new Phase1AdminRequests(request).getFieldsAsList(format));
        }
        final Text csvText = new Text(writer.toString().trim());
        context.write(csvText, NullWritable.get());
      }
    }
  }

}
