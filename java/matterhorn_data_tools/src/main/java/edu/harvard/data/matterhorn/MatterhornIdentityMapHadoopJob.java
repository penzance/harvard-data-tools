package edu.harvard.data.matterhorn;

import java.io.IOException;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.identity.IdentityMapHadoopJob;

public class MatterhornIdentityMapHadoopJob extends IdentityMapHadoopJob {
  public static void main(final String[] args) throws IOException, DataConfigurationException {
    new MatterhornIdentityMapHadoopJob(args[0]).run();
  }

  public MatterhornIdentityMapHadoopJob(final String configPathString)
      throws IOException, DataConfigurationException {
    super(MatterhornDataConfig.parseInputFiles(MatterhornDataConfig.class, configPathString, true),
        new MatterhornCodeManager());
  }

}
