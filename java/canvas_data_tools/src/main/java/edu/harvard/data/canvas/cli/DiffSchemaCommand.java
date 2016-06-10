package edu.harvard.data.canvas.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.kohsuke.args4j.Argument;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.schema.SchemaDifference;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DiffSchemaCommand implements Command {

  @Argument(index = 0, usage = "Schema version 1.", metaVar = "1.0.0", required = true)
  private String v1;

  @Argument(index = 1, usage = "Schema version 2.", metaVar = "1.1.0", required = true)
  private String v2;

  @Override
  public ReturnStatus execute(final CanvasDataConfig config, final ExecutorService exec) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final ApiClient api = new ApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    final CanvasDataSchema s1 = (CanvasDataSchema) api.getSchema(v1);
    final CanvasDataSchema s2 = (CanvasDataSchema) api.getSchema(v2);
    System.out.println("Former schema: " + v1);
    System.out.println("New schema: " + v2);
    for (final SchemaDifference diff : s1.diff(s2)) {
      System.out.println(diff);
      System.out.println();
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Compare two named schema versions.";
  }

}
