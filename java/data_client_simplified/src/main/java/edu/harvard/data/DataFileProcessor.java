package edu.harvard.data;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public class DataFileProcessor implements Callable<Void> {

  private final S3ObjectId input;
  private final S3ObjectId output;
  private final IdentityService idService;
  private final DataConfig config;
  private final CodeManager codeManager;
  private final String tableName;
  private final TableFormat format;

  public DataFileProcessor(final S3ObjectId input, final S3ObjectId output, final String tableName,
      final IdentityService idService, final CodeManager codeManager, final DataConfig config) {
    this.input = input;
    this.output = output;
    this.tableName = tableName;
    this.idService = idService;
    this.codeManager = codeManager;
    this.config = config;
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
    System.out.println("Infile: " + input + " outfile: " + output);
  }

  @Override
  public Void call() throws Exception {
    final File tmpDir = new File(config.getScratchDir(), tableName);
    final File tmpFile = new File(tmpDir, UUID.randomUUID().toString());
    try (
        TableReader<? extends DataTable> in = codeManager.getS3InputTableReader(tableName, format,
            input, tmpDir);
        CloseableMap<String, TableWriter<DataTable>> outputs = codeManager.getWriters(tableName,
            format, output, tmpFile)) {
      final List<ProcessingStep> steps = new ArrayList<ProcessingStep>();
      addStep(steps, codeManager.getIdentityStep(tableName, idService, config));
      addStep(steps, codeManager.getFullTextStep(tableName));
      final Map<String, List<ProcessingStep>> customSteps = codeManager.getCustomSteps(config);
      if (customSteps.containsKey(tableName)) {
        for (final ProcessingStep step : customSteps.get(tableName)) {
          addStep(steps, step);
        }
      }
      final TableWriter<DataTable> out = outputs.getMap().get(tableName);
      for (final DataTable inputRecord : in) {
        DataTable record = inputRecord;
        for (final ProcessingStep step : steps) {
          record = step.process(record, outputs);
        }
        out.add(record);
      }
    }
    return null;
  }

  private void addStep(final List<ProcessingStep> steps, final ProcessingStep step) {
    if (step != null) {
      steps.add(step);
    }
  }

}
