package edu.harvard.data.matterhorn.phase_2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.matterhorn.MatterhornDataConfiguration;

public class Phase2HadoopManager {
  public static List<Job> getJobs(final AwsUtils aws, final Configuration hadoopConfig,
      final MatterhornDataConfiguration config, final URI hdfsService, final String inputDir,
      final String outputDir) throws DataConfigurationException, IOException {
    hadoopConfig.set("format", Format.DecompressedCanvasDataFlatFiles.toString());
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new VideoJob(hadoopConfig, aws, hdfsService, inputDir, outputDir).getJob());
    jobs.add(new SessionJob(hadoopConfig, aws, hdfsService, inputDir, outputDir).getJob());
    jobs.add(new EventTypeJob(hadoopConfig, aws, hdfsService, inputDir, outputDir).getJob());
    return jobs;
  }

}
