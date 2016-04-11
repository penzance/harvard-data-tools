package edu.harvard.data.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

public class HdfsTableReader<T extends DataTable> implements TableReader<T> {

  private final Class<T> tableType;
  private final HdfsDelimitedFileIterator<T> iterator;

  public HdfsTableReader(final Class<T> tableType, final TableFormat format, final FileSystem fs,
      final Path path) throws IOException {
    this.tableType = tableType;
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

  @Override
  public Class<T> getTableType() {
    return tableType;
  }

}

class HdfsDelimitedFileIterator<T extends DataTable> extends DelimitedFileIterator<T> {

  private final FileSystem fs;
  private final Path path;

  public HdfsDelimitedFileIterator(final Class<T> tableType, final TableFormat format,
      final FileSystem fs, final Path path) throws IOException {
    super(tableType, format, null);
    this.fs = fs;
    this.path = path;
  }

  @Override
  InputStream getInputStream() throws IOException {
    if (inStream != null) {
      return inStream;
    }
    final FSDataInputStream inStream = fs.open(path);
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
