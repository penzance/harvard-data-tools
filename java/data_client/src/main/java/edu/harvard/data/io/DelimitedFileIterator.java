package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * Helper class that implements an iterator over comma, tab or otherwise
 * delimited data files. The delimiter character, as well as other
 * characteristics of the data file, are indicated by the {@link TableFormat}
 * object passed to the constructor.
 *
 * The iterator reads each line of the source file and reflectively calls the
 * {@link CSVRecord} constructor on the appropriate {@link DataTable} class that
 * parses a line in the input to populate an object.
 *
 * The iterator does not cache any records, meaning that its memory footprint is
 * small.
 *
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 *
 * Note that the iteration process can throw an instance of
 * {@link IterationException}. This occurs when an exception is encountered
 * inside the {@link java.util.Iterator#hasNext} or
 * {@link java.util.Iterator#next} methods. Since the {@code Iterator} interface
 * does not declare any checked exceptions, we instead wrap any exceptions in a
 * runtime exception and rely on the calling code to catch them.
 *
 * @param <T>
 *          the {@link DataTable} implementation to be read by this iterator.
 */
class DelimitedFileIterator<T extends DataTable> implements Iterator<T>, Closeable {

  private Iterator<CSVRecord> iterator;
  private CSVParser requestParser;
  protected final Class<T> table;
  protected final TableFormat format;
  private final File file;
  protected InputStream inStream;
  private int line;

  /**
   * Create a new iterator.
   *
   * @param tableType
   *          a reference to the template class {@code T} that will be used to
   *          create new records.
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param file
   *          a {@link File} object that refers to the data file.
   */
  public DelimitedFileIterator(final Class<T> tableType, final TableFormat format,
      final File file) {
    this.format = format;
    this.table = tableType;
    this.file = file;
  }

  /**
   * Build a CSV parser that sits on top of a file input stream. If the file
   * format includes headers, this method skips the first line of the input,
   * since we do not use the file headers for schema information.
   *
   * @throws IOException
   *           if an error occurs when reading the file or creating the parser.
   */
  private void createIterator() throws IOException {
    if (iterator == null) {
      final InputStream in = getInputStream();
      requestParser = new CSVParser(new InputStreamReader(in, format.getEncoding()),
          format.getCsvFormat());
      iterator = requestParser.iterator();
      if (format.includeHeaders()) {
        iterator.next();
        line = 1;
      } else {
        line = 0;
      }
    }
  }

  /**
   * Open the appropriate {@link InputStream} type, depending on whether the
   * format calls for compression or not.
   *
   * @return an {@code InputStream} on top of the local file that can be parsed
   *         for records.
   *
   * @throws IOException if an error occurs creating the stream.
   */
  protected InputStream getInputStream() throws IOException {
    if (inStream != null) {
      return inStream;
    }
    return format.getInputStream(file);
  }

  @Override
  public boolean hasNext() {
    if (iterator == null) {
      try {
        createIterator();
      } catch (final IOException e) {
        throw new IterationException(e);
      }
    }
    return iterator.hasNext();
  }

  @Override
  public T next() {
    if (iterator == null) {
      try {
        createIterator();
      } catch (final IOException e) {
        throw new IterationException(e);
      }
    }
    final CSVRecord next = iterator.next();
    line++;
    try {
      return table.getConstructor(TableFormat.class, CSVRecord.class).newInstance(format, next);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    } catch (final InvocationTargetException e) {
      Throwable cause = e;
      while (cause instanceof InvocationTargetException) {
        cause = cause.getCause();
      }
      throw new IterationException(cause);
    }
  }

  @Override
  public void close() throws IOException {
    if (requestParser != null) {
      requestParser.close();
    }
    if (inStream != null) {
      inStream.close();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}