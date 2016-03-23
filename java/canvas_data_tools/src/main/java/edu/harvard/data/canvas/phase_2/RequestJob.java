package edu.harvard.data.canvas.phase_2;

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

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.UserAgentParser;
import edu.harvard.data.canvas.bindings.phase1.Phase1Requests;
import edu.harvard.data.canvas.bindings.phase2.Phase2Requests;
import net.sf.uadetector.ReadableUserAgent;

class RequestJob extends HadoopJob {

  public RequestJob(final Configuration hadoopConf, final DataConfiguration dataConfig,
      final AwsUtils aws, final URI hdfsService, final String inputDir, final String outputDir) {
    super(hadoopConf, aws, hdfsService, inputDir, outputDir);
  }

  @Override
  public Job getJob() throws IOException {
    final Job job = Job.getInstance(hadoopConf, "requests-hadoop");
    job.setMapperClass(RequestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, aws, hdfsService, inputDir + "/requests", outputDir + "/requests");
    return job;
  }
}

class RequestMapper extends Mapper<Object, Text, Text, NullWritable> {

  private final TableFormat format;
  private final UserAgentParser uaParser;

  public RequestMapper() throws IOException, DataConfigurationException {
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
    this.uaParser = new UserAgentParser();
  }

  @Override
  public void map(final Object key, final Text value, final Context context)
      throws IOException, InterruptedException {
    final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
    for (final CSVRecord csvRecord : parser.getRecords()) {
      final Phase1Requests request = new Phase1Requests(format, csvRecord);
      //      if (request.getUserId() == null || (request.getUserId() != -262295411484124942L
      //          || request.getUserId() == 134926641248969922L)) {
      final Phase2Requests extended = new Phase2Requests(request);
      parseUserAgent(extended);

      final StringWriter writer = new StringWriter();
      try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
        printer.printRecord(extended.getFieldsAsList(format));
      }
      final Text csvText = new Text(writer.toString().trim());
      context.write(csvText, NullWritable.get());
      //      }
    }
  }

  private void parseUserAgent(final Phase2Requests request) {
    final String agentString = request.getUserAgent();
    if (agentString != null) {
      final ReadableUserAgent agent = uaParser.parse(agentString);
      request.setBrowser(agent.getName());
      request.setOs(agent.getOperatingSystem().getName());
    }
  }

}
