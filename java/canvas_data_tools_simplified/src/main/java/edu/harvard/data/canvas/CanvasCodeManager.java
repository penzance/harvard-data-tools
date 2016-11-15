package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DownloadAndVerify;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.canvas.bindings.phase_0.Phase0CanvasTableFactory;
import edu.harvard.data.canvas.bindings.phase_1.Phase1CanvasTableFactory;
import edu.harvard.data.canvas.steps.Phase2RequestsStep;
import edu.harvard.data.canvas.steps.Phase3AdminRequestsStep;

public class CanvasCodeManager extends CodeManager {

  public CanvasCodeManager() {
    super(new CanvasGeneratedCodeManager(), new Phase0CanvasTableFactory(),
        new Phase1CanvasTableFactory());
  }

  @Override
  public DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException {
    return CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString, verify);
  }

  @Override
  public DownloadAndVerify getDownloadAndVerify(final DataConfig config, final String datasetId,
      final String runId, final ExecutorService exec)
          throws IOException, DataConfigurationException {
    return new CanvasDownloadAndVerify((CanvasDataConfig) config, runId, datasetId, exec);
  }

  @Override
  public Map<String, List<ProcessingStep>> getCustomSteps(final DataConfig config) {
    final Map<String, List<ProcessingStep>> steps = new HashMap<String, List<ProcessingStep>>();
    steps.put("requests", new ArrayList<ProcessingStep>());
    steps.get("requests").add(new Phase2RequestsStep());
    steps.get("requests").add(new Phase3AdminRequestsStep());
    return steps;
  }
}
