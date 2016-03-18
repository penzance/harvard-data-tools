package edu.harvard.data.io;

import java.io.Closeable;

import edu.harvard.data.DataTable;

public interface TableReader<T extends DataTable> extends Iterable<T>, Closeable {

  Class<T> getTableType();

}
