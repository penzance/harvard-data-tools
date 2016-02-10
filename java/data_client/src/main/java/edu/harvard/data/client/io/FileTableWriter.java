package edu.harvard.data.client.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVPrinter;

import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFormat;

public class FileTableWriter<T extends DataTable> implements TableWriter<T> {

  private static final int DEFAULT_BUFFER_SIZE = 512;

  private final List<T> buffer;
  private int bufferSize;
  private final String tableName;
  private final File file;
  private final TableFormat format;
  private final Class<T> tableType;
  CSVPrinter printer;

  public FileTableWriter(final Class<T> tableType, final TableFormat format, final String tableName,
      final File file) {
    this.tableName = tableName;
    this.buffer = new ArrayList<T>();
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.file = file;
    this.tableType = tableType;
    this.format = format;
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
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
    if (printer != null) {
      flush();
      printer.close();
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void flush() throws IOException {
    if (printer == null) {
      printer = new CSVPrinter(new OutputStreamWriter(format.getOutputStream(file)),format.getCsvFormat());
      if (format.includeHeaders()) {
        writeHeaders(printer);
      }
    }
    for (final T row : buffer) {
      printer.printRecord(row.getFieldsAsList(format));
    }
    buffer.clear();
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
