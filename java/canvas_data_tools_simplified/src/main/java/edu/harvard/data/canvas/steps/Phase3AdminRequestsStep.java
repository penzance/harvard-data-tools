package edu.harvard.data.canvas.steps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import edu.harvard.data.CloseableMap;
import edu.harvard.data.DataTable;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.canvas.CanvasCodeGenerator;
import edu.harvard.data.canvas.bindings.phase_2.Phase2Requests;
import edu.harvard.data.canvas.bindings.phase_3.Phase3AdminRequests;
import edu.harvard.data.canvas.bindings.phase_3.Phase3Requests;
import edu.harvard.data.io.TableWriter;

public class Phase3AdminRequestsStep implements ProcessingStep {

  private final Set<String> adminUuids;

  public Phase3AdminRequestsStep() {
    this.adminUuids = new HashSet<String>();
  }

  @Override
  public Void call() throws IOException {
    final String resource = CanvasCodeGenerator.ADMIN_UUID_RESOURCE;
    final ClassLoader cl = this.getClass().getClassLoader();
    try (final BufferedReader in = new BufferedReader(
        new InputStreamReader(cl.getResourceAsStream(resource)))) {
      adminUuids.add(in.readLine());
    }
    return null;
  }

  @Override
  public DataTable process(final DataTable record,
      final CloseableMap<String, TableWriter<DataTable>> extraOutputs) throws IOException {
    final Phase2Requests in = (Phase2Requests) record;
    final Phase3Requests out = new Phase3Requests(in);
    if (adminUuids.contains(in.getUserIdResearchUuid())
        || adminUuids.contains(in.getRealUserIdResearchUuid())) {
      extraOutputs.getMap().get("admin_requests").add(new Phase3AdminRequests(out));
      return null;
    } else {
      return out;
    }
  }

}
