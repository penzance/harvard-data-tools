package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

public class FileTableReader<T extends DataTable> implements TableReader<T> {

  private final Class<T> tableType;
  private final Iterator<T> iterator;

  public FileTableReader(final Class<T> tableType, final TableFormat format, final File file)
      throws IOException {
    this.tableType = tableType;
    if (!file.exists() || file.isDirectory()) {
      throw new FileNotFoundException(file.toString());
    }
    iterator = new DelimitedFileIterator<T>(tableType, format, file);
  }

  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    ((Closeable) iterator).close();
  }

  @Override
  public Class<T> getTableType() {
    return tableType;
  }

}
