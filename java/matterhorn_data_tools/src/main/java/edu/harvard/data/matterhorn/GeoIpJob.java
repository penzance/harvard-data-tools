package edu.harvard.data.matterhorn;

import java.io.IOException;

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
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.TableFormat;
import edu.harvard.data.matterhorn.bindings.phase1.Phase1GeoIp;
import edu.harvard.data.matterhorn.bindings.phase2.Phase2GeoIp;

public class GeoIpJob extends HadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final int phase = Integer.parseInt(args[1]);
    final MatterhornDataConfig config = MatterhornDataConfig
        .parseInputFiles(MatterhornDataConfig.class, configPathString, true);
    new GeoIpJob(config, phase).runJob();
  }

  public GeoIpJob(final DataConfig config, final int phase) throws DataConfigurationException {
    super(config, phase);
  }

  @Override
  public Job getJob() throws IOException, NoInputDataException {
    final Job job = Job.getInstance(hadoopConf, "geoip-hadoop");
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(GeoIpMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(GeoIpReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    final String inputDir = config.getHdfsDir(phase - 1);
    final String outputDir = config.getHdfsDir(phase);
    hadoopUtils.setPaths(job, hdfsService, inputDir + "/geo_ip", outputDir + "/geo_ip");
    return job;
  }
}

class GeoIpMapper extends Mapper<Object, Text, Text, Text> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public GeoIpMapper() {
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
    final Phase1GeoIp geoIp = new Phase1GeoIp(format, parser.getRecords().get(0));
    context.write(new Text(geoIp.getIp()), hadoopUtils.convertToText(geoIp, format));
  }
}

class GeoIpReducer extends Reducer<Text, Text, Text, NullWritable> {

  private TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public GeoIpReducer() {
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
    final Phase2GeoIp geoIp = new Phase2GeoIp();
    geoIp.setIp(key.toString());
    for (final Text value : values) {
      final CSVParser parser = CSVParser.parse(value.toString(), format.getCsvFormat());
      final Phase1GeoIp g = new Phase1GeoIp(format, parser.getRecords().get(0));
      if (geoIp.getAreaCode() == null && g.getAreaCode() != null) {
        geoIp.setAreaCode(g.getAreaCode());
      }
      if (geoIp.getCityName() == null && g.getCityName() != null) {
        geoIp.setCityName(g.getCityName());
      }
      if (geoIp.getContinentCode() == null && g.getContinentCode() != null) {
        geoIp.setContinentCode(g.getContinentCode());
      }
      if (geoIp.getCountryCode2() == null && g.getCountryCode2() != null) {
        geoIp.setCountryCode2(g.getCountryCode2());
      }
      if (geoIp.getCountryCode3() == null && g.getCountryCode3() != null) {
        geoIp.setCountryCode3(g.getCountryCode3());
      }
      if (geoIp.getCountryName() == null && g.getCountryName() != null) {
        geoIp.setCountryName(g.getCountryName());
      }
      if (geoIp.getDmaCode() == null && g.getDmaCode() != null) {
        geoIp.setDmaCode(g.getDmaCode());
      }
      if (geoIp.getLatitude() == null && g.getLatitude() != null) {
        geoIp.setLatitude(g.getLatitude());
      }
      if (geoIp.getLongitude() == null && g.getLongitude() != null) {
        geoIp.setLongitude(g.getLongitude());
      }
      if (geoIp.getPostalCode() == null && g.getPostalCode() != null) {
        geoIp.setPostalCode(g.getPostalCode());
      }
      if (geoIp.getRealRegionName() == null && g.getRealRegionName() != null) {
        geoIp.setRealRegionName(g.getRealRegionName());
      }
      if (geoIp.getRegionName() == null && g.getRegionName() != null) {
        geoIp.setRegionName(g.getRegionName());
      }
      if (geoIp.getTimezone() == null && g.getTimezone() != null) {
        geoIp.setTimezone(g.getTimezone());
      }
    }
    context.write(hadoopUtils.convertToText(geoIp, format), NullWritable.get());
  }
}
