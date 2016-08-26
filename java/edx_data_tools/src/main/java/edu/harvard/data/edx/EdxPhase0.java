package edu.harvard.data.edx;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.Phase0;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.pipeline.InputTableIndex;

public class EdxPhase0 extends Phase0 {

  private static final Logger log = LogManager.getLogger();
  private final EdxDataConfig config;
  private final String runId;
  private final ExecutorService exec;

  // Main class for testing only.
  public static void main(final String[] args) throws Exception {
    final String configPathString = args[0];
    final String runId = args[1];
    final String datasetId = args[2];
    final int threads = Integer.parseInt(args[3]);
    final String codeManagerClassName = args[4];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
    final DataConfig config = codeManager.getDataConfig(configPathString, true);

    ReturnStatus status;
    ExecutorService exec = null;
    try {
      exec = Executors.newFixedThreadPool(threads);
      final EdxPhase0 phase0 = (EdxPhase0) codeManager.getPhase0(configPathString, datasetId, runId,
          exec);
      status = phase0.run();
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
    System.exit(status.getCode());
  }

  public EdxPhase0(final EdxDataConfig config, final String runId, final ExecutorService exec) {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
  }

  @Override
  protected ReturnStatus run() throws IOException, InterruptedException, ExecutionException {
    final File inFile = new File("/tmp/data/harvardx-edx-events-2016-08-23.log");
    final File outFile = new File("/tmp/data/events-2016-08-23.gz");
    final InputTableIndex dataIndex = new InputTableIndex();
    final List<Future<InputTableIndex>> jobs = new ArrayList<Future<InputTableIndex>>();

    final EdxSingleFileParser parser = new EdxSingleFileParser(inFile, outFile, config);
    jobs.add(exec.submit(parser));

    for (final Future<InputTableIndex> job : jobs) {
      dataIndex.addAll(job.get());
    }
    dataIndex.setSchemaVersion("1.0");
    for (final String table : dataIndex.getTableNames()) {
      dataIndex.setPartial(table, true);
    }

    return ReturnStatus.OK;
  }
}

class EdxSingleFileParser implements Callable<InputTableIndex> {
  private static final Logger log = LogManager.getLogger();

  private final EdxDataConfig config;
  private final File inputFile;
  private final File outputFile;

  public EdxSingleFileParser(final File inputFile, final File outputFile,
      final EdxDataConfig config) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.config = config;
  }

  @Override
  public InputTableIndex call() throws Exception {
    log.info("Parsing file " + inputFile + " and writing to " + outputFile);
    final InputParser parser = new InputParser(config, inputFile, outputFile);
    return parser.parseFile();
  }

}
