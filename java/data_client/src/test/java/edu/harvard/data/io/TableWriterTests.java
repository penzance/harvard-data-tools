package edu.harvard.data.io;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;

public class TableWriterTests {

  private OutputStream out;
  private TableFormat format;
  private TableFormat mockFormat;
  private BufferedReader reader;
  private File file;
  private File parentFile;

  @Before
  public void setup() throws IOException {
    final PipedInputStream in = new PipedInputStream();
    out = new PipedOutputStream(in);
    final FormatLibrary formatLibrary = new FormatLibrary();
    format = formatLibrary.getFormat(Format.DecompressedCanvasDataFlatFiles);
    reader = new BufferedReader(new InputStreamReader(in));
    file = mock(File.class);
    parentFile = mock(File.class);
    when(file.exists()).thenReturn(false);
    when(file.getParentFile()).thenReturn(parentFile);
    mockFormat = mock(TableFormat.class);
    when(mockFormat.getOutputStream(file)).thenReturn(out);
    when(mockFormat.includeHeaders()).thenReturn(false);
    when(mockFormat.getCsvFormat()).thenReturn(format.getCsvFormat());

  }

  @After
  public void tearDown() throws IOException {
    reader.close();
  }

  private TableWriter<DataTableStub> makeWriter(final OutputStream outStream) {
    return new TableWriter<DataTableStub>(DataTableStub.class, format, outStream);
  }

  private TableWriter<DataTableStub> makeWriter(final File outFile) {
    return new TableWriter<DataTableStub>(DataTableStub.class, format, outFile);
  }

  private int countRecordsWritten() throws IOException {
    int lines = 0;
    String line = reader.readLine();
    while (line != null) {
      lines++;
      line = reader.readLine();
    }
    return lines;
  }

  // Check the expected behavior for writer.
  @Test
  public void testOutput() throws IOException {
    final DataTableStub record = new DataTableStub(format);
    final TableWriter<DataTableStub> writer = makeWriter(out);
    writer.add(record);
    writer.close();
    assertEquals(record.recordString(), reader.readLine());
  }

  // Check that an existing file is deleted
  @Test
  public void testDeletionOfExistingFile() {
    when(file.exists()).thenReturn(true);
    makeWriter(file);
    verify(file, times(1)).delete();
  }

  // Check that any required directories are created on demand
  @Test
  public void testCreationOfParentFiles() {
    makeWriter(file);
    verify(parentFile, times(1)).mkdirs();
  }

  // Check that an output stream is created for a file
  @Test
  public void testFileOutputStream() throws IOException {
    final TableWriter<DataTableStub> writer = new TableWriter<DataTableStub>(DataTableStub.class,
        mockFormat, file);
    writer.add(new DataTableStub(mockFormat));
    writer.close();
    verify(mockFormat, times(1)).getOutputStream(file);
  }

  // Check that records are written in order
  @Test
  public void testRecordsWrittenInOrder() throws IOException {
    final DataTableStub record1 = new DataTableStub(format);
    final DataTableStub record2 = new DataTableStub(format, 123, "S1", "S2", null, null);
    final TableWriter<DataTableStub> writer = makeWriter(out);
    writer.add(record1);
    writer.add(record2);
    writer.close();
    assertEquals(record1.recordString(), reader.readLine());
    assertEquals(record2.recordString(), reader.readLine());
  }

  // Check that only the records passed are written
  @Test
  public void testNoExtraRecords() throws IOException {
    final TableWriter<DataTableStub> writer = makeWriter(out);
    writer.add(new DataTableStub(format));
    writer.add(new DataTableStub(format));
    writer.close();
    assertEquals(2, countRecordsWritten());
  }

  // Check that no records are lost when the buffer is resized
  @Test
  public void testBufferResize() throws IOException {
    final TableWriter<DataTableStub> writer = makeWriter(out);
    writer.add(new DataTableStub(format));
    writer.add(new DataTableStub(format));
    writer.resizeBuffer(1);
    writer.add(new DataTableStub(format));
    writer.close();
    assertEquals(3, countRecordsWritten());
  }

  // Check that the output stream is not closed when close method is called
  @Test(expected = IOException.class)
  public void testOutputStreamClosed() throws IOException {
    final TableWriter<DataTableStub> writer = makeWriter(out);
    writer.close();
    out.write(0);
  }

  // Check that required headers are written
  @Test
  public void testHeadersWritten() throws IOException {
    when(mockFormat.includeHeaders()).thenReturn(true);
    final TableWriter<DataTableStub> writer = new TableWriter<DataTableStub>(DataTableStub.class,
        mockFormat, out);
    final DataTableStub record = new DataTableStub(mockFormat, 123, "S1", "S2", null, null);
    writer.add(record);
    writer.close();
    assertEquals(DataTableStub.headerString(), reader.readLine());
    assertEquals(record.recordString(), reader.readLine());
  }

  // Check that required headers are written even if there are no records
  @Test
  public void testHeadersWrittenWithoutRecords() throws IOException {
    when(mockFormat.includeHeaders()).thenReturn(true);
    final TableWriter<DataTableStub> writer = new TableWriter<DataTableStub>(DataTableStub.class,
        mockFormat, out);
    writer.close();
    assertEquals(DataTableStub.headerString(), reader.readLine());
  }

}