package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVPrinter;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * Output stream that writes a series of {@link DataTable} records to an
 * {@link OutputStream} or to a local file. The records will be written in the
 * same order that they were received by the {@link #add} method.
 * <P>
 * Clients of this class should ensure that the {@link #close} method is called
 * when all records have been added to the writer. The class performs buffering
 * to cut down on the number of writes, and so may not write the final records
 * if {@code close} is not called.
 * <P>
 * The memory footprint of this writer is designed to be small. Aside from
 * records that are buffered for performance reasons (up to 512 records by
 * default, although this can be changed by the {@link #resizeBuffer} method),
 * no other records are stored to memory.
 * <P>
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 * <P>
 * A single {@code TableWriter} instance outputs a single type of
 * {@link DataTable} records, determined by the type parameter {@code T}.
 */
public class TableWriter<T extends DataTable> implements Closeable {

  private static final int DEFAULT_BUFFER_SIZE = 512;

  private final List<T> buffer;
  private int bufferSize;
  private File file;
  private final TableFormat format;
  private final Class<T> tableType;
  private OutputStream outStream;
  private CSVPrinter printer;

  /**
   * Common internal constructor that sets up standard fields regardless of the
   * data source.
   */
  private TableWriter(final Class<T> tableType, final TableFormat format) {
    this.buffer = new ArrayList<T>();
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.tableType = tableType;
    this.format = format;
  }

  /**
   * Create a new {@code TableWriter} object that output records to a local
   * file. Any necessary parent directories will be created, and any existing
   * file with the same name will be overwritten.
   *
   * @param tableType
   *          the type of record that will be written by this
   *          {@code TableWriter}.
   * @param format
   *          the format (represented by a {@link TableFormat} object) of the
   *          output files. This format determines characteristics of the output
   *          such as the delimiter between columns, the presence of headers or
   *          the compression algorithm used.
   * @param file
   *          a local {@link File} object to which the records will be written.
   *          The writer will overwrite any existing file in that location.
   */
  public TableWriter(final Class<T> tableType, final TableFormat format, final File file) {
    this(tableType, format);
    this.file = file;
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
  }

  /**
   * Create a new {@code TableWriter} object that output records to an output
   * stream.
   *
   * @param tableType
   *          the type of record that will be written by this
   *          {@code TableWriter}.
   * @param format
   *          the format (represented by a {@link TableFormat} object) of the
   *          output files. This format determines characteristics of the output
   *          such as the delimiter between columns, the presence of headers or
   *          the compression algorithm used.
   * @param outStream
   *          an open {@link OutputStream} to which the records will be written.
   *          This stream will be closed when the {@link #close} method is
   *          called on this instance.
   */
  public TableWriter(final Class<T> tableType, final TableFormat format,
      final OutputStream outStream) {
    this(tableType, format);
    this.outStream = outStream;
  }

  /**
   * Write a record to the output stream or file. The order of records in the
   * output will match the order in which this method was called (note that
   * there is no synchronization performed by this class - any concurrency must
   * be managed by the caller).
   *
   * @param record
   *          the data to be written.
   * @throws IOException
   *           if an error occurs when writing to the output stream or local
   *           file. Note that due to buffering not every invocation of this
   *           method triggers a write to the output stream or local file, so
   *           callers cannot assume that all records have been safely written
   *           until the {@link #close} method has been successfully called.
   */
  public void add(final T record) throws IOException {
    buffer.add(record);
    if (buffer.size() >= bufferSize) {
      flush();
    }
  }

  /**
   * Write any remaining records that are still in the output buffer, and close
   * any internal streams. This method closes an {@code OutputStream} passed to
   * the constructor.
   */
  @Override
  public void close() throws IOException {
    flush();
    if (printer != null) {
      printer.close();
    }
  }

  /**
   * Change the size of the output buffer. Records are stored in a local buffer
   * between writes as a performance optimization. The buffer is flushed when it
   * reaches the maximum buffer size (defaulting to 512 records). Changing this
   * value will cause writes to be more or less frequent, with consequences for
   * performance and for memory footprint.
   *
   * Note that the buffer size only affects caching in the {@code TableWriter}.
   * Depending on the {@code OutputStream} types used, there may be additional
   * caching elsewhere in the system.
   *
   * This method triggers a flush of the output buffer.
   *
   * @param bufferSize
   *          the new size of the buffer, measured in the number of records
   *          stored between writes.
   * @throws IOException
   *           if an error occurs when flushing the buffer to the output stream
   *           or local file.
   */
  public void resizeBuffer(final int bufferSize) throws IOException {
    this.bufferSize = bufferSize;
    flush();
  }

  /**
   * Write all records stored in the output buffer to the output stream or local
   * file. This method will create a new {@code CSVPrinter} object on top of the
   * output stream if one does not exist already, and will write headers if the
   * file format requires it.
   *
   * @throws IOException
   *           if an error occurs while writing the records or creating an
   *           output stream.
   */
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

  /**
   * Create a {@link CSVPrinter} object on top of the output stream (either
   * user-provided or newly-created from a local file).
   *
   * @throws IOException
   *           if an error occurs when creating an output stream or constructing
   *           the CSV printer.
   */
  private void getPrinter() throws IOException {
    final OutputStream out;
    if (outStream == null) {
      out = format.getOutputStream(file);
    } else {
      out = outStream;
    }
    printer = new CSVPrinter(new OutputStreamWriter(out), format.getCsvFormat());
  }

  /**
   * Output the headers to the data stream. This method should only be called
   * once for the output file (since there should only be one set of headers
   * written), and only if the {@link TableFormat#includeHeaders} method returns
   * true.
   *
   * @param printer
   *          the {@link CSVPrinter} stream where the headers should be written.
   * @throws IOException
   *           if an error occurs when writing to the print stream.
   * @throws RuntimeException
   *           if an error occurs when reflectively accessing the field names
   *           from the tableType object. Since we don't necessarily have a
   *           record object available when writing the table headers (a
   *           {@code TableWriter} could be created that writes headers but no
   *           data), we need to use a static method on the {@link DataTable}
   *           subtype to fetch the field names. We can't call a static method
   *           on a generic {@code T} type (since the compiler can't check
   *           whether that static method exists), so we have to use reflection.
   *           An exception thrown here represents an error in the internal
   *           logic for this class, and so it's unreasonable to expect the
   *           caller to handle it.
   */
  private void writeHeaders(final CSVPrinter printer) throws IOException {
    try {
      final T table = tableType.newInstance();
      final List<String> headers = table.getFieldNames();
      printer.printRecord(headers);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
