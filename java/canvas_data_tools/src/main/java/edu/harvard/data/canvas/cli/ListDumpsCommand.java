package edu.harvard.data.canvas.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfiguration;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class ListDumpsCommand implements Command {

  @Override
  public ReturnStatus execute(final CanvasDataConfiguration config, final ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final ApiClient api = new ApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    for (final DataDump dump : api.getDumps()) {
      System.out.println(dump);
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Get a JSON list of tables, with the latest non-incremental dump";
  }

}
