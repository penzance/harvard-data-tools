package edu.harvard.data.data_tools.canvas;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.harvard.data.client.DataClient;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.CanvasApiClient;
import edu.harvard.data.client.canvas.CanvasDataDump;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.data_tools.Command;
import edu.harvard.data.data_tools.ReturnStatus;
import edu.harvard.data.data_tools.VerificationException;

public class CanvasListDumpsCommand implements Command {

  @Override
  public ReturnStatus execute(final DataConfiguration config, final ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final CanvasApiClient api = DataClient.getCanvasApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    for (final CanvasDataDump dump : api.getDumps()) {
      System.out.println(dump);
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Get a JSON list of tables, with the latest non-incremental dump";
  }

}
