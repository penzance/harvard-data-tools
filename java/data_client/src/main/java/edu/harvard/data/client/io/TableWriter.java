package edu.harvard.data.client.io;

import java.io.Closeable;
import java.io.IOException;

import edu.harvard.data.client.DataTable;

public interface TableWriter<T extends DataTable> extends Closeable {

  void add(DataTable a) throws IOException;

  String getTableName();

  void flush() throws IOException;

  void pipe(TableReader<T> in) throws IOException;

}
