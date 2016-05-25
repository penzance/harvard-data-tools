package edu.harvard.data.io;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;

/**
 * Iterator that reads JSON-formated data from a local file. This class parses
 * each line of the input file as a JSON object, and then defers to a
 * {@link JsonDocumentParser} instance to convert that JSON object to a set of
 * one or more {@link DataTable} instances.
 *
 * The iterator does not cache any records, meaning that its memory footprint is
 * small.
 *
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 *
 * This class should not be instantiated by clients; create an instance of
 * {@link JsonFileReader} instead.
 *
 * Note that the iteration process can throw an instance of
 * {@link IterationException}. This occurs when an exception is encountered
 * inside the {@link java.util.Iterator#hasNext} or
 * {@link java.util.Iterator#next} methods. Since the {@code Iterator} interface
 * does not declare any checked exceptions, we instead wrap any exceptions in a
 * runtime exception and rely on the calling code to catch them.
 */
public class JsonFileIterator
implements Iterator<Map<String, List<? extends DataTable>>>, Closeable {

  protected final TableFormat format;
  private final File file;
  protected BufferedReader in;
  private String nextLine;
  private final JsonDocumentParser parser;
  private InputStream inStream;

  /**
   * Create a new iterator over a local data file.
   *
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted. For a JSON document, the CSV-related fields of the
   *          format are ignored, and the {@code TableFormat} is used to parse
   *          data fields (such as timestamps) and to determine the encoding and
   *          compression used in the file.
   * @param file
   *          a {@link File} reference to a local data file.
   * @param parser
   *          an implementation of the {@link JsonDocumentParser} interface that
   *          is able to convert a parsed JSON object into a set of all
   *          {@link DataTable} records contained within that JSON object.
   */
  JsonFileIterator(final TableFormat format, final File file, final JsonDocumentParser parser) {
    this.format = format;
    this.parser = parser;
    this.file = file;
  }

  /**
   * Create a new iterator over an input stream.
   *
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted. For a JSON document, the CSV-related fields of the
   *          format are ignored, and the {@code TableFormat} is used to parse
   *          data fields (such as timestamps) and to determine the encoding and
   *          compression used in the file.
   * @param inStream
   *          an {@link InputStream} from which data can be read. This stream
   *          will be closed when the {@link #close} method is called.
   * @param parser
   *          an implementation of the {@link JsonDocumentParser} interface that
   *          is able to convert a parsed JSON object into a set of all
   *          {@link DataTable} records contained within that JSON object.
   */
  public JsonFileIterator(final TableFormat format, final InputStream inStream,
      final JsonDocumentParser parser) {
    this(format, (File) null, parser);
    this.inStream = inStream;
  }

  @Override
  public boolean hasNext() {
    // Call init() if it has not been called already.
    if (in == null) {
      try {
        init();
      } catch (final IOException e) {
        throw new IterationException(e);
      }
    }
    // Init sets nextLine to be the first line in the file (or null for an empty
    // file). next() sets nextLine to be the next line in the file, or null if
    // there are no more records.
    return nextLine != null;
  }

  /**
   * Set up the input streams that will be used to read the data file. This
   * method should be called exactly once, and only before attempting to read a
   * record from the file.
   *
   * @throws IOException
   *           if an error occurs when setting up the input streams.
   */
  private void init() throws IOException {
    if (inStream == null) {
      inStream = format.getInputStream(file);
    }
    in = new BufferedReader(new InputStreamReader(inStream));
    nextLine = in.readLine();
  }

  @Override
  public Map<String, List<? extends DataTable>> next() {
    // call init() if it hasn't been called already
    if (in == null) {
      try {
        init();
      } catch (final IOException e) {
        throw new IterationException(e);
      }
    }

    // nextLine is set by init() or by the previous time through this method.
    if (nextLine == null) {
      return null;
    }

    // JSON mapper accepts a file or a stream; in this case we convert the
    // String containing a single object into a ByteArrayInputStream.
    final InputStream lineIn = new ByteArrayInputStream(nextLine.getBytes());
    final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
    };
    try {
      // Parse the String containing a JSON object, and pass the result to the
      // JsonDocumentParser to split into DataTables.
      final Map<String, Object> obj = format.getJsonMapper().readValue(lineIn, typeRef);
      final Map<String, List<? extends DataTable>> documents = parser.getDocuments(obj);

      // Advance the iterator to the next line in the input file.
      nextLine = in.readLine();

      // Return the parsed DataTables.
      return documents;
    } catch (final IOException | ParseException | VerificationException e) {
      throw new IterationException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    } else if (inStream != null) {
      inStream.close();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}