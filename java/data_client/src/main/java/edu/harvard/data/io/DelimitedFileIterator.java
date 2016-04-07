package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.RecordParsingException;
import edu.harvard.data.TableFormat;

class DelimitedFileIterator<T extends DataTable> implements Iterator<T>, Closeable {

  private Iterator<CSVRecord> iterator;
  private CSVParser requestParser;
  protected final Class<T> table;
  protected final TableFormat format;
  private final File file;
  protected InputStream inStream;
  private int line;

  public DelimitedFileIterator(final Class<T> table, final TableFormat format, final File file)
      throws IOException {
    this.format = format;
    this.table = table;
    this.file = file;
    if (format.includeHeaders()) {
      iterator.next();
      line = 1;
    } else {
      line = 0;
    }
  }

  private void createIterator() {
    try {
      final InputStream in = getInputStream();
      requestParser = new CSVParser(new InputStreamReader(in, format.getEncoding()),
          format.getCsvFormat());
      iterator = requestParser.iterator();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  InputStream getInputStream() throws IOException {
    if (inStream != null) {
      return inStream;
    }
    switch (format.getCompression()) {
    case Gzip:
      return new GZIPInputStream(new FileInputStream(file));
    case None:
      return new FileInputStream(file);
    default:
      throw new RuntimeException("Unknown compression format: " + format.getCompression());
    }
  }

  @Override
  public boolean hasNext() {
    if (iterator == null) {
      createIterator();
    }
    return iterator.hasNext();
  }

  @Override
  public T next() {
    if (iterator == null) {
      createIterator();
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
      throw new RecordParsingException(file, line, next, cause);
    }
  }

  @Override
  public void close() throws IOException {
    if (requestParser != null) {
      requestParser.close();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}