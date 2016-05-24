package edu.harvard.data.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Before;
import org.junit.Test;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.TableFormat.Compression;

public class DelimitedFileIteratorTests {

  private TableFormat format;
  private File file;
  private PrintWriter writer;
  private TableFormat noHeaderFormat;
  private InputStream in;
  private OutputStream out;

  @Before
  public void setup() throws IOException {
    out = new PipedOutputStream();
    in = new PipedInputStream((PipedOutputStream) out);
    writer = new PrintWriter(new OutputStreamWriter(out));
    final FormatLibrary formatLibrary = new FormatLibrary();
    noHeaderFormat = formatLibrary.getFormat(Format.DecompressedCanvasDataFlatFiles);

    file = mock(File.class);
    when(file.exists()).thenReturn(true);

    format = mock(TableFormat.class);
    when(format.getInputStream(file)).thenReturn(in);
    when(format.getCompression()).thenReturn(Compression.None);
    when(format.includeHeaders()).thenReturn(false);
    when(format.getEncoding()).thenReturn(noHeaderFormat.getEncoding());
    when(format.getCsvFormat()).thenReturn(noHeaderFormat.getCsvFormat());
    final DataTableStub base = new DataTableStub(format);
    when(format.formatTimestamp(base.date)).thenReturn(noHeaderFormat.formatTimestamp(base.date));
    when(format.formatTimestamp(base.timestamp))
    .thenReturn(noHeaderFormat.formatTimestamp(base.timestamp));
    when(format.getDateFormat()).thenReturn(noHeaderFormat.getDateFormat());
    when(format.getTimstampFormat()).thenReturn(noHeaderFormat.getTimstampFormat());
  }

  private List<DataTableStub> writeRecords(final int count) {
    final List<DataTableStub> lst = new ArrayList<DataTableStub>();
    final DataTableStub base = new DataTableStub(format);
    for (int i = 0; i < count; i++) {
      final DataTableStub record = new DataTableStub(format, i, base.string1 + i, base.string2 + i,
          base.timestamp, base.date);
      writer.write(record.recordString() + "\n");
      lst.add(record);
    }
    writer.close();
    return lst;
  }

  // Check the common path.
  @Test
  public void testFileReading() throws IOException {
    try (final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);) {
      final List<DataTableStub> records = writeRecords(10);
      final List<DataTableStub> read = new ArrayList<DataTableStub>();
      while (it.hasNext()) {
        read.add(it.next());
      }
      assertEquals(10, read.size());
      for (int i = 0; i < read.size(); i++) {
        assertEquals(records.get(i), read.get(i));
      }
    }
  }

  // Check that an empty file exits properly
  @Test
  public void testEmptyFile() throws IOException {
    try (final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);) {
      writer.close();
      assertFalse(it.hasNext());
    }
  }

  // Check that input streams are closed properly
  @Test(expected = IOException.class)
  public void testStreamsClosed() throws IOException {
    final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);
    writeRecords(1);
    while (it.hasNext()) {
      it.next();
    }
    it.close();
    in.read();
  }

  // Check that if the format specifies a header row it is skipped.
  @Test
  public void testHeaderSkipped() throws IOException {
    when(format.includeHeaders()).thenReturn(true);
    try (final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);) {
      final List<DataTableStub> records = writeRecords(2);
      final List<DataTableStub> read = new ArrayList<DataTableStub>();
      while (it.hasNext()) {
        read.add(it.next());
      }
      assertEquals(1, read.size());
      assertEquals(records.get(1), read.get(0));
    }
  }

  // Check that if the format doesn't specify a header row no data is skipped.
  @Test
  public void testHeaderNotSkipped() throws IOException {
    when(format.includeHeaders()).thenReturn(false);
    try (final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);) {
      final List<DataTableStub> records = writeRecords(1);
      final List<DataTableStub> read = new ArrayList<DataTableStub>();
      while (it.hasNext()) {
        read.add(it.next());
      }
      assertEquals(1, read.size());
      assertEquals(records.get(0), read.get(0));
    }
  }

  // Check that a compressed input stream is read properly
  @Test
  public void testCompressedFile() throws IOException {
    out = new PipedOutputStream();
    in = new PipedInputStream((PipedOutputStream) out);
    writer = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(out)));
    when(format.getCompression()).thenReturn(Compression.Gzip);
    when(format.getInputStream(file)).thenReturn(new GZIPInputStream(in));

    try (final DelimitedFileIterator<DataTableStub> it = new DelimitedFileIterator<DataTableStub>(
        DataTableStub.class, format, file);) {
      final List<DataTableStub> records = writeRecords(10);
      final List<DataTableStub> read = new ArrayList<DataTableStub>();
      while (it.hasNext()) {
        read.add(it.next());
      }
      assertEquals(10, read.size());
      for (int i = 0; i < read.size(); i++) {
        assertEquals(records.get(i), read.get(i));
      }
    }
  }
}
