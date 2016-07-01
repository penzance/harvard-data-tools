package edu.harvard.data.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;

public class JsonFileIteratorTests {

  private TableFormat format;
  private FormatLibrary formatLibrary;
  private DataTableStubDocumentParser parser;
  private PipedInputStream in;
  private PipedOutputStream out;
  private JsonFileReader reader;

  @Before
  public void setup() throws IOException {
    out = new PipedOutputStream();
    in = new PipedInputStream(out);
    parser = new DataTableStubDocumentParser();
    formatLibrary = new FormatLibrary();
    format = formatLibrary.getFormat(Format.DecompressedInternal);
    reader = new JsonFileReader(format, in, parser);
  }

  private List<DataTableStub> writeObjects(final int count) throws JsonProcessingException {
    final List<DataTableStub> lst = DataTableStub.generateRecords(count, format);
    final PrintWriter writer = new PrintWriter(new OutputStreamWriter(out));
    for (final DataTableStub record : lst) {
      final String json = format.getJsonMapper().writeValueAsString(record);
      writer.println(json);
    }
    writer.close();
    return lst;
  }

  @SuppressWarnings("unchecked")
  private void compareToSeen(final List<DataTableStub> written, final int i) {
    assertEquals(written.get(i).int1,
        ((Map<String, Object>) parser.seen.get(i).get("fieldsAsMap")).get("int_1"));
  }

  // Check expected path
  @Test
  public void testJsonFileIterator() throws IOException {
    final List<DataTableStub> written = writeObjects(2);
    final List<Map<String, List<? extends DataTable>>> records = new ArrayList<Map<String, List<? extends DataTable>>>();
    for (final Map<String, List<? extends DataTable>> r : reader) {
      records.add(r);
    }
    reader.close();
    assertEquals(written.size(), records.size());
    for (int i = 0; i < records.size(); i++) {
      compareToSeen(written, i);
    }
  }

  // Call next() without calling hasNext() to check that the input streams are
  // properly initialized.
  @Test
  public void testSkipHasNext() throws IOException {
    final List<DataTableStub> written = writeObjects(2);
    final List<Map<String, List<? extends DataTable>>> records = new ArrayList<Map<String, List<? extends DataTable>>>();
    final Iterator<Map<String, List<? extends DataTable>>> it = reader.iterator();
    Map<String, List<? extends DataTable>> r = it.next();
    while (r != null) {
      records.add(r);
      r = it.next();
    }
    reader.close();
    assertEquals(written.size(), records.size());
    for (int i = 0; i < records.size(); i++) {
      compareToSeen(written, i);
    }
  }

  // Check with empty input stream
  @Test
  public void testEmptyInputStream() throws IOException {
    final List<Map<String, List<? extends DataTable>>> records = new ArrayList<Map<String, List<? extends DataTable>>>();
    out.close();
    for (final Map<String, List<? extends DataTable>> r : reader) {
      records.add(r);
    }
    reader.close();
    assertEquals(0, records.size());
  }

  // Check with non-existent file
  @Test(expected = FileNotFoundException.class)
  public void testMissingFile() throws IOException {
    reader = new JsonFileReader(format, new File("/tmp/" + UUID.randomUUID()), parser);
    reader.close();
  }

  // Check that input stream is closed after close method called
  @Test(expected = IOException.class)
  public void testInputStreamClosed() throws IOException {
    writeObjects(2);
    reader.close();
    in.read();
  }
}

class DataTableStubDocumentParser implements JsonDocumentParser {

  List<Map<String, Object>> seen;
  int idx;

  public DataTableStubDocumentParser() {
    this.seen = new ArrayList<Map<String, Object>>();
    this.idx = 0;
  }

  @Override
  public Map<String, List<? extends DataTable>> getDocuments(final Map<String, Object> values)
      throws ParseException, VerificationException {
    seen.add(values);
    return new HashMap<String, List<? extends DataTable>>();
  }

}