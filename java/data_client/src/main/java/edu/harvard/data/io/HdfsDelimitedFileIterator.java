package edu.harvard.data.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * Helper class that iterates over a data file stored on the Hadoop Distributed
 * File System (HDFS). This class inherits the majority of its functionality
 * from {@link DelimitedFileIterator}, changing only its input from the local
 * file system to HDFS.
 *
 * This class should not be instantiated by clients; create an instance of
 * {@link HdfsTableReader} instead.
 *
 * @param <T>
 *          the {@link DataTable} implementation to be read by this iterator.
 */
public class HdfsDelimitedFileIterator<T extends DataTable> extends DelimitedFileIterator<T> {

  private final FileSystem fs;
  private final Path path;

  /**
   * Create a new iterator.
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
   */
  HdfsDelimitedFileIterator(final Class<T> tableType, final TableFormat format,
      final FileSystem fs, final Path path) {
    super(tableType, format, null);
    this.fs = fs;
    this.path = path;
  }

  /**
   * Open the appropriate {@link InputStream} type, depending on whether the
   * format calls for compression or not.
   *
   * @return an {@code InputStream} on top of the HDFS file specified in the
   *         constructor that can be parsed for records.
   *
   * @throws IOException
   *           if an error occurs creating the stream.
   */
  @Override
  protected InputStream getInputStream() throws IOException {
    if (inStream != null) {
      return inStream;
    }
    final InputStream inStream = fs.open(path);
    switch (format.getCompression()) {
    case Gzip:
      return new GZIPInputStream(inStream);
    case None:
      return inStream;
    default:
      throw new RuntimeException("Unknown compression format: " + format.getCompression());
    }
  }

}