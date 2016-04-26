package edu.harvard.data.canvas.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.kohsuke.args4j.Argument;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfiguration;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DumpInfoCommand implements Command {

  @Argument(index = 0, usage = "Sequence number.", metaVar = "123", required = true)
  public long sequence;

  @Override
  public ReturnStatus execute(final CanvasDataConfiguration config, final ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final ApiClient api = new ApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    System.out.println(api.getDump(sequence));
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Get details of a particular dump.";
  }

}
