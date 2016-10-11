package edu.harvard.data;

import java.io.IOException;

import edu.harvard.data.io.TableWriter;

public class DataOutput<T extends DataTable> {

  public DataOutput(final DataConfig config, final Class<T> cls) {
  }

  public void output(final T record, final CloseableMap<String, TableWriter<T>> writers)
      throws IOException {
    for (final TableWriter<T> writer : writers.getMap().values()) {
      writer.add(record);
    }
  }
}
