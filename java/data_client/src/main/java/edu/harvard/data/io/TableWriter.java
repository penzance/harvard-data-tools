package edu.harvard.data.io;

import java.io.Closeable;
import java.io.IOException;

import edu.harvard.data.DataTable;

public interface TableWriter<T extends DataTable> extends Closeable {

  void add(DataTable a) throws IOException;

  void pipe(TableReader<T> in) throws IOException;

}
