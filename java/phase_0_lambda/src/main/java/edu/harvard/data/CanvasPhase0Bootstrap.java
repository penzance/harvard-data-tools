package edu.harvard.data;

import java.io.IOException;

import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0Bootstrap {
  public static void main(final String[] args)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        args[0], true);
    final ApiClient api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
    for (final DataDump dump : api.getDumps()) {
      System.out.println(dump);
    }
  }
}
