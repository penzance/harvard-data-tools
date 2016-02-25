package edu.harvard.data.data_tools.canvas;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.data_tools.Command;
import edu.harvard.data.data_tools.ReturnStatus;
import edu.harvard.data.data_tools.VerificationException;

public class CanvasDataCommandGroup implements Command {

  private static final Logger log = LogManager.getLogger();

  @Argument(handler = SubCommandHandler.class, usage = "Canvas data operation.")
  @SubCommands({ @SubCommand(name = "download", impl = CanvasDownloadDumpCommand.class),
    @SubCommand(name = "verify", impl = CanvasVerifyDumpCommand.class),
    @SubCommand(name = "hadoop", impl = CanvasHadoopCommand.class),
    @SubCommand(name = "redshift", impl = CanvasUpdateRedshiftCommand.class),
    @SubCommand(name = "compareschemas", impl = CanvasCompareSchemasCommand.class), })
  public Command cmd;

  @Override
  public String getDescription() {
    return "Commands for managing the Canvas data set";
  }

  @Override
  public ReturnStatus execute(final DataConfiguration config) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    log.info("Running command: " + cmd.getDescription());
    return cmd.execute(config);
  }
}
