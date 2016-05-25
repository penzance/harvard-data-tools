package edu.harvard.data.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * {@link TableReader} implementation that reads records from a delimited data
 * file.
 * <P>
 * This class is a simple wrapper around {@link DelimitedFileIterator}; see the
 * documentation for that class for details around how a file is read.
 * <P>
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 *
 * @param <T>
 *          the record type that this file reader parses.
 */
public class FileTableReader<T extends DataTable> implements TableReader<T> {

  private final DelimitedFileIterator<T> iterator;

  /**
   * Create a new reader.
   *
   * @param tableType
   *          a reference to the template class {@code T} that will be used to
   *          create new records.
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param file
   *          a {@link File} object that refers to the data file.
   *
   * @throws FileNotFoundException
   *           if the file parameter refers to a file that does not exist.
   */
  public FileTableReader(final Class<T> tableType, final TableFormat format, final File file)
      throws FileNotFoundException {
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
    iterator.close();
  }

}
