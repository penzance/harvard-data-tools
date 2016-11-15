package edu.harvard.data;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableWriter;

public interface GeneratedCodeManager {

  ProcessingStep getFullTextStep(String tableName);

  ProcessingStep getIdentityStep(String tableName, IdentityService idService, DataConfig config);

  Map<String, TableWriter<DataTable>> getFullTextWriters(final TableFactory tableFactory,
      String tableName, TableFormat format, String tmpFileBase) throws IOException;

  Set<ProcessingStep> getAllSteps();

}
