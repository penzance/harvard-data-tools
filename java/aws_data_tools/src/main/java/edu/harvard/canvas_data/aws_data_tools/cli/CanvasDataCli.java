package edu.harvard.canvas_data.aws_data_tools.cli;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import edu.harvard.canvas_data.aws_data_tools.DumpInfo;
import edu.harvard.canvas_data.aws_data_tools.FatalError;
import edu.harvard.canvas_data.aws_data_tools.VerificationException;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.api.UnexpectedApiResponseException;

public class CanvasDataCli {

  private static final Logger log = LogManager.getLogger();

  @Argument(handler = SubCommandHandler.class, usage = "Top-level command.")
  @SubCommands({ @SubCommand(name = "download", impl = DownloadDumpCommand.class),
    @SubCommand(name = "verify", impl = VerifyDumpCommand.class),
    @SubCommand(name = "compareschemas", impl = CompareSchemasCommand.class), })
  public Command cmd;

  public static void main(final String[] args) {
    final CanvasDataCli parser = new CanvasDataCli();
    final CmdLineParser cli = new CmdLineParser(parser);
    try {
      cli.parseArgument(args);
    } catch (final CmdLineException e) {
      System.exit(ReturnStatus.ARGUMENT_ERROR.getCode());
    }
    if (parser.cmd == null) {
      System.exit(ReturnStatus.ARGUMENT_ERROR.getCode());
    } else {
      DataConfiguration config = null; // Config is set or System.exit is
      // called.
      try {
        config = DataConfiguration.getConfiguration("secure.properties");
        DumpInfo.init(config.getDumpInfoDynamoTable());
        log.info("Using table " + config.getDumpInfoDynamoTable() + " for dump info.");
      } catch (final DataConfigurationException e) {
        log.fatal("Invalid configuration. Field", e);
        System.exit(ReturnStatus.CONFIG_ERROR.getCode());
      } catch (final IOException e) {
        log.fatal("IO error when reading configuration: " + e.getMessage(), e);
        System.exit(ReturnStatus.IO_ERROR.getCode());
      }
      try {
        final ReturnStatus status = parser.cmd.execute(config);
        if (status.isFailure()) {
          bail(status, args, config, "Task resulted in unexpected status code.");
        }
      } catch (final IOException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.IO_ERROR, args, config, "IO error: " + e.getMessage());
      } catch (final VerificationException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.VERIFICATION_FAILURE, args, config, "Verification error: " + e.getMessage());
      } catch (final IllegalArgumentException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.ARGUMENT_ERROR, args, config, e.getMessage());
      } catch (final UnexpectedApiResponseException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.API_ERROR, args, config, "API error: " + e.getMessage());
      } catch (final FatalError e) {
        log.fatal(e.getMessage(), e);
        bail(e.getStatus(), args, config, e.getMessage());
      } catch (final Throwable t) {
        log.fatal(t.getMessage(), t);
        bail(ReturnStatus.UNKNOWN_ERROR, args, config, "Unexpected error: " + t.getMessage());
      }
    }
  }

  public static void bail(final ReturnStatus status, final String[] args, final DataConfiguration config, final String message) {
    log.error("Exiting with error status " + status);
    log.error(message);
    log.error("Application was launched with arguments:");
    for (final String arg : args) {
      log.error("  " + arg);
    }
    log.error("Canvas data host: " + config.getCanvasDataHost());
    log.error("DynamoDB dump info table: " + config.getDumpInfoDynamoTable());
    log.error("Local scratch directory: " + config.getScratchDir());
    log.error("S3 archive location: " + config.getCanvasDataArchiveKey());
    System.exit(status.getCode());
  }
}
