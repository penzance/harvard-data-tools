package edu.harvard.data.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * Implementation of the {@link TableReader} interface that reads records from a
 * file stored on the Hadoop Distributed File System.
 * <P>
 * This class is a simple wrapper around {@link HdfsDelimitedFileIterator}; see
 * the documentation for that class for details around how a file is read.
 *
 * @param <T>
 *          the record type that this file reader parses.
 */
public class HdfsTableReader<T extends DataTable> implements TableReader<T> {

  private final HdfsDelimitedFileIterator<T> iterator;

  /**
   * Create a new reader.
   *
   * @param tableType
   *          a reference to the template class {@code T} that will be used to
   *          create new records.
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param fs
   *          The Hadoop file system on which the data file is stored.
   * @param path
   *          the location of the data file on the Hadoop file system.
   *
   * @throws IOException
   *           if an error occurs when reading from HDFS.
   */
  public HdfsTableReader(final Class<T> tableType, final TableFormat format, final FileSystem fs,
      final Path path) throws IOException {
    if (!fs.exists(path) || fs.isDirectory(path)) {
      throw new FileNotFoundException(path.toString());
    }
    iterator = new HdfsDelimitedFileIterator<T>(tableType, format, fs, path);
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
