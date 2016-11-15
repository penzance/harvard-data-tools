package edu.harvard.data;

import java.io.IOException;
import java.util.concurrent.Callable;

import edu.harvard.data.io.TableWriter;

public interface ProcessingStep extends Callable<Void> {
  DataTable process(final DataTable record, CloseableMap<String, TableWriter<DataTable>> extraOutputs) throws IOException;
}
