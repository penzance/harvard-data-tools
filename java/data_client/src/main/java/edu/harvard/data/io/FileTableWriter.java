package edu.harvard.data.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVPrinter;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

public class FileTableWriter<T extends DataTable> implements TableWriter<T> {

  private static final int DEFAULT_BUFFER_SIZE = 512;

  private final List<T> buffer;
  private int bufferSize;
  private File file;
  private final TableFormat format;
  private final Class<T> tableType;
  private OutputStream outStream;
  private CSVPrinter printer;


  public FileTableWriter(final Class<T> tableType, final TableFormat format) {
    this.buffer = new ArrayList<T>();
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.tableType = tableType;
    this.format = format;
  }

  public FileTableWriter(final Class<T> tableType, final TableFormat format,
      final File file) {
    this(tableType, format);
    this.file = file;
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
  }

  public FileTableWriter(final Class<T> tableType, final TableFormat format,
      final OutputStream outStream) {
    this(tableType, format);
    this.outStream = outStream;
  }

  public void setBufferSize(final int size) {
    this.bufferSize = size;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(final DataTable a) throws IOException {
    buffer.add((T) a);
    if (buffer.size() > bufferSize) {
      flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (!buffer.isEmpty()) {
      flush();
    }
    if (printer != null) {
      printer.close();
    }
  }

  private void flush() throws IOException {
    if (printer == null) {
      getPrinter();
      if (format.includeHeaders()) {
        writeHeaders(printer);
      }
    }
    for (final T row : buffer) {
      printer.printRecord(row.getFieldsAsList(format));
    }
    buffer.clear();
  }

  private void getPrinter() throws IOException {
    final OutputStream out;
    if (outStream == null) {
      out = format.getOutputStream(file);
    } else {
      out = outStream;
    }
    printer = new CSVPrinter(new OutputStreamWriter(out),format.getCsvFormat());
  }

  @SuppressWarnings("unchecked")
  private void writeHeaders(final CSVPrinter printer) throws IOException {
    try {
      final List<String> headers = (List<String>) tableType.getMethod("getFieldNames").invoke(null);
      printer.printRecord(headers);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pipe(final TableReader<T> in) throws IOException {
    for (final T t : in) {
      add(t);
    }
    flush();
  }

}
