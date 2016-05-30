package edu.harvard.data.canvas.phase_2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.canvas.CanvasDataConfig;

public class Phase2HadoopManager {
  public static List<Job> getJobs(final AwsUtils aws, final Configuration hadoopConfig,
      final CanvasDataConfig config, final URI hdfsService, final String inputDir,
      final String outputDir) throws DataConfigurationException, IOException {
    hadoopConfig.set("format", Format.DecompressedCanvasDataFlatFiles.toString());
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new RequestJob(hadoopConfig, config, aws, hdfsService, inputDir, outputDir).getJob());
    jobs.add(new AdminRequestJob(hadoopConfig, config, aws, hdfsService, inputDir, outputDir).getJob());
    return jobs;
  }

}
