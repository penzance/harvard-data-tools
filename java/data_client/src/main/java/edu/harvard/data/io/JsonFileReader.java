package edu.harvard.data.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

public class JsonFileReader implements Closeable, Iterable<Map<String, ? extends DataTable>> {

  private final JsonFileIterator iterator;

  public JsonFileReader(final TableFormat format, final File file, final JsonDocumentParser parser)
      throws IOException {
    if (!file.exists() || file.isDirectory()) {
      throw new FileNotFoundException(file.toString());
    }
    iterator = new JsonFileIterator(format, file, parser);
  }

  @Override
  public Iterator<Map<String, ? extends DataTable>> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    ((Closeable) iterator).close();
  }

}
