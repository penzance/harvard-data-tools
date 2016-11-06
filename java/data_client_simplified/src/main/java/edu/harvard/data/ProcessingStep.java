package edu.harvard.data;

import java.io.IOException;

import edu.harvard.data.io.TableWriter;

public interface ProcessingStep {
  DataTable process(final DataTable record, CloseableMap<String, TableWriter<DataTable>> extraOutputs) throws IOException;
}
