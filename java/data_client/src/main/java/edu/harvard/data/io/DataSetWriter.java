package edu.harvard.data.io;

import java.io.Closeable;
import java.io.IOException;

import edu.harvard.data.DataTable;

public interface DataSetWriter extends Closeable {
  void pipe(DataSetReader reader) throws IOException;

  <T extends DataTable> TableWriter<T> getTable(String table, Class<T> tableClass) throws IOException;

  <T extends DataTable> void setTableWriter(String tableName, Class<T> tableClass, TableWriter<T> table);

  void flush() throws IOException;
}
