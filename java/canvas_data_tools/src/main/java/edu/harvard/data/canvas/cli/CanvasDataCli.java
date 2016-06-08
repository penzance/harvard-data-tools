package edu.harvard.data.canvas.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasDataCli {
  private static final Logger log = LogManager.getLogger();

  @Argument(index = 0, usage = "Colon-separated list of config files.", metaVar = "/path/to/config1|/path/to/config2", required = true)
  private String configPaths;

  @Argument(index = 1, handler = SubCommandHandler.class, usage = "Canvas data operation.")
  @SubCommands({ @SubCommand(name = "download", impl = DownloadDumpCommand.class),
    @SubCommand(name = "preverify", impl = PreVerifyCommand.class),
    @SubCommand(name = "postverify", impl = PostVerifyCommand.class),
    @SubCommand(name = "hadoop", impl = HadoopCommand.class),
    @SubCommand(name = "updateredshift", impl = UpdateRedshiftCommand.class),
    @SubCommand(name = "unload", impl = UnloadExistingTablesCommand.class),
    @SubCommand(name = "cleanup", impl = CleanupCommand.class),
    @SubCommand(name = "fulldumps", impl = GetCompleteTableInfoCommand.class),
    @SubCommand(name = "listdumps", impl = ListDumpsCommand.class),
    @SubCommand(name = "dumpinfo", impl = DumpInfoCommand.class),
    @SubCommand(name = "diffschemas", impl = DiffSchemaCommand.class),
    @SubCommand(name = "compareschemas", impl = CompareSchemasCommand.class), })
  public Command cmd;

  @Option(name = "-threads", usage = "Number of threads to use for concurrent jobs. Defaults to 1", metaVar = "threadCount", required = false)
  public Integer threads = 1;

  public static void main(final String[] args) {
    log.info("Launched Data CLI");
    final CanvasDataCli parser = new CanvasDataCli();
    final CmdLineParser cli = new CmdLineParser(parser);
    try {
      cli.parseArgument(args);
    } catch (final CmdLineException e) {
      log.fatal("Command line exception", e);
      printUsage(cli);
      System.exit(ReturnStatus.ARGUMENT_ERROR.getCode());
    }
    if (parser.cmd == null) {
      log.fatal("Invalid command line arguments.");
      printUsage(cli);
      System.exit(ReturnStatus.ARGUMENT_ERROR.getCode());
    } else {
      // Config is set or System.exit is called.
      CanvasDataConfig config = null;
      try {
        config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, parser.configPaths, true);
        DumpInfo.init(config.dumpInfoDynamoTable);
        TableInfo.init(config.tableInfoDynamoTable);
        log.info("Using table " + config.dumpInfoDynamoTable + " for dump info.");
      } catch (final DataConfigurationException e) {
        log.fatal("Invalid configuration. Field", e);
        System.exit(ReturnStatus.CONFIG_ERROR.getCode());
      } catch (final IOException e) {
        log.fatal("IO error when reading configuration: " + e.getMessage(), e);
        System.exit(ReturnStatus.IO_ERROR.getCode());
      }
      ExecutorService exec = null;
      try {
        log.info("Using up to " + parser.threads + " threads");
        exec = Executors.newFixedThreadPool(parser.threads);
        final ReturnStatus status = parser.cmd.execute(config, exec);
        if (status.isFailure()) {
          bail(status, args, config, "Task resulted in unexpected status code.", null);
        }
      } catch (final IOException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.IO_ERROR, args, config, "IO error: " + e.getMessage(), e);
      } catch (final VerificationException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.VERIFICATION_FAILURE, args, config,
            "Verification error: " + e.getMessage(), e);
      } catch (final IllegalArgumentException e) {
        log.fatal(e.getMessage(), e);
        printUsage(cli);
        bail(ReturnStatus.ARGUMENT_ERROR, args, config, e.getMessage(), e);
      } catch (final UnexpectedApiResponseException e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.API_ERROR, args, config, "API error: " + e.getMessage(), e);
      } catch (final ArgumentError e) {
        log.fatal(e.getMessage(), e);
        bail(ReturnStatus.ARGUMENT_ERROR, args, config, e.getMessage(), e);
      } catch (final Throwable t) {
        log.fatal(t.getMessage(), t);
        bail(ReturnStatus.UNKNOWN_ERROR, args, config, "Unexpected error: " + t.getMessage(), t);
      } finally {
        if (exec != null) {
          final List<Runnable> jobs = exec.shutdownNow();
          if (jobs.size() > 0) {
            log.error("Shutting down execution framework with " + jobs.size() + " jobs remaining");
          }
        }
      }
    }
  }

  private static void printUsage(final CmdLineParser cli) {
    log.info("usage: java -jar " + CanvasDataCli.class.getCanonicalName()
        + " <data_set> <operation> <args>");
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    cli.printUsage(out);
    for (final String line : bytes.toString().split("\n")) {
      log.info(line);
    }
  }

  public static void bail(final ReturnStatus status, final String[] args,
      final CanvasDataConfig config, final String message, final Throwable t) {
    log.error("Exiting with error status " + status);
    if (t == null) {
      log.error(message);
    } else {
      log.error(message, t);
    }
    log.error("Application was launched with arguments:");
    for (final String arg : args) {
      log.error("  " + arg);
    }
    log.error("Canvas data host: " + config.canvasDataHost);
    log.error("DynamoDB dump info table: " + config.dumpInfoDynamoTable);
    log.error("Local scratch directory: " + config.scratchDir);
    System.exit(status.getCode());
  }
}
