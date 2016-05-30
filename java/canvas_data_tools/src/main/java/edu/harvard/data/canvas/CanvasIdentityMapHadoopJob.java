package edu.harvard.data.canvas;

import java.io.IOException;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.identity.IdentityMapHadoopJob;

public class CanvasIdentityMapHadoopJob extends IdentityMapHadoopJob {

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    new CanvasIdentityMapHadoopJob(args[0]).run();
  }

  public CanvasIdentityMapHadoopJob(final String configPathString)
      throws IOException, DataConfigurationException {
    super(CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString, true),
        new CanvasCodeManager());
  }

}
