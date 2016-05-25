package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * Stream class that parses an {@link InputStream} or local {@link File}
 * containing JSON-formatted records. This class does not implement
 * {@link TableReader}, since it is possible for one JSON record to produce many
 * {@link DataTable} objects.
 *
 * This class is a wrapper around {@link JsonFileIterator}; see the
 * documentation for that class for more details.
 */
public class JsonFileReader implements Closeable, Iterable<Map<String, List<? extends DataTable>>> {

  private final JsonFileIterator iterator;

  /**
   * Create a new reader based on a local file.
   *
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param file
   *          a {@link File} object that refers to the data file.
   * @param parser
   *          an implementation of the {@link JsonDocumentParser} interface that
   *          is able to convert a parsed JSON object into a set of all
   *          {@link DataTable} records contained within that JSON object.
   *
   * @throws FileNotFoundException
   *           if the file parameter refers to a file that does not exist.
   */
  public JsonFileReader(final TableFormat format, final File file, final JsonDocumentParser parser)
      throws FileNotFoundException {
    if (!file.exists() || file.isDirectory()) {
      throw new FileNotFoundException(file.toString());
    }
    iterator = new JsonFileIterator(format, file, parser);
  }

  /**
   * Create a new reader based on an {@link InputStream}.
   *
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param inStream
   *          an {@link InputStream} from which JSON-formatted records can be
   *          read. The stream will be closed when the {@link #close} method is
   *          called on this instance.
   * @param parser
   *          an implementation of the {@link JsonDocumentParser} interface that
   *          is able to convert a parsed JSON object into a set of all
   *          {@link DataTable} records contained within that JSON object.
   */
  public JsonFileReader(final TableFormat format, final InputStream inStream,
      final JsonDocumentParser parser) {
    iterator = new JsonFileIterator(format, inStream, parser);
  }

  @Override
  public Iterator<Map<String, List<? extends DataTable>>> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    ((Closeable) iterator).close();
  }

}
