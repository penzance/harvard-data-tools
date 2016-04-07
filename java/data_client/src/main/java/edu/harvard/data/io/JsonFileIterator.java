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
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;

class JsonFileIterator implements Iterator<Map<String, ? extends DataTable>>, Closeable {

  protected final TableFormat format;
  private final File file;
  protected BufferedReader in;
  private String nextLine;
  private final JsonDocumentParser parser;

  public JsonFileIterator(final TableFormat format, final File file, final JsonDocumentParser parser)
      throws IOException {
    this.format = format;
    this.parser = parser;
    this.file = file;
  }

  @Override
  public boolean hasNext() {
    if (in == null) {
      try {
        init();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
    return nextLine != null;
  }

  private void init() throws IOException {
    in = new BufferedReader(new InputStreamReader(format.getInputStream(file)));
    nextLine = in.readLine();
  }

  @Override
  public Map<String, ? extends DataTable> next() {
    if (nextLine == null) {
      return null;
    }
    final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
    };
    final InputStream lineIn = new ByteArrayInputStream(nextLine.getBytes());
    try {
      nextLine = in.readLine();
      final Map<String, Object> obj = format.getJsonMapper().readValue(lineIn, typeRef);
      return parser.getDocuments(obj);
    } catch (final IOException | ParseException | VerificationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}