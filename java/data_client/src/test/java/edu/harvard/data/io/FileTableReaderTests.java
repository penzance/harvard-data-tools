package edu.harvard.data.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

// The majority of the work for this class is done by the DelimitedFileIterator;
// see DelimitedFileIteratorTests for tests of that functionality
public class FileTableReaderTests {
  // Check exception when using a missing file
  @Test(expected = FileNotFoundException.class)
  public void testMissingFile() throws IOException {
    final FileTableReader<DataTableStub> reader = new FileTableReader<DataTableStub>(
        DataTableStub.class, null, new File("/tmp/" + UUID.randomUUID()));
    reader.close();
  }
}
