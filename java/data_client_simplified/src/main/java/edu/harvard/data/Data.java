package edu.harvard.data;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.identity.IdentityService;

public class Data {
  public static void main(final String[] args) throws Throwable {
    final String configPathString = args[0];
    final String runId = args[1];
    final String datasetId = args[2];
    final int threads = Integer.parseInt(args[3]);
    final String codeManagerClassName = args[4];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
    final DataConfig config = codeManager.getDataConfig(configPathString, true);
    final S3ObjectId workingLocation = config.getS3WorkingLocation(runId);

    ExecutorService exec = null;
    try (IdentityService idService = new IdentityService(config, workingLocation)) {
      exec = Executors.newFixedThreadPool(threads);
      final InputTableIndex tableIndex = codeManager
          .getDownloadAndVerify(config, datasetId, runId, exec).run();
      final Map<String, List<String>> tables = tableIndex.getTables();
      runSetupMethods(exec, idService, codeManager, config);

      final Set<Future<Void>> jobs = new HashSet<Future<Void>>();
      for (final String tableName : tables.keySet()) {
        for (final String inFile : tables.get(tableName)) {
          final String fileName = inFile.substring(inFile.lastIndexOf("/") + 1);
          final S3ObjectId outFile = AwsUtils.key(workingLocation, "redshift_staging", tableName, fileName);
          final Callable<Void> job = new DataFileProcessor(AwsUtils.key(inFile), outFile,
              tableName, idService, codeManager, config);
          jobs.add(exec.submit(job));
        }
      }
      for (final Future<Void> job : jobs) {
        job.get();
      }
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  private static void runSetupMethods(final ExecutorService exec, final IdentityService idService,
      final CodeManager codeManager, final DataConfig config) throws Throwable {
    final Set<Future<Void>> jobs = new HashSet<Future<Void>>();
    jobs.add(exec.submit(idService));
    for (final ProcessingStep step : codeManager.getAllSteps(config)) {
      jobs.add(exec.submit(step));
    }
    for (final Future<Void> future : jobs) {
      try {
        future.get();
      } catch (final InterruptedException e) {
        throw e;
      } catch (final ExecutionException e) {
        throw e.getCause();
      }
    }
  }
}
