package edu.harvard.data.client.io;

import java.io.Closeable;

import edu.harvard.data.client.DataTable;

public interface TableReader<T extends DataTable> extends Iterable<T>, Closeable {

  Class<T> getTableType();

}
