package edu.harvard.data.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;

public class CombinedTableIteratorTests {

  private List<TableReader<DataTableStub>> getReaders(final Integer... sizes) {
    final List<TableReader<DataTableStub>> readers = new ArrayList<TableReader<DataTableStub>>();
    for (int i = 0; i < sizes.length; i++) {
      readers.add(new TableReaderStub<DataTableStub>(sizes[i], "t" + i));
    }
    return readers;
  }

  // Check expected behavior
  @Test
  public void testCombinedIterator() throws IOException {
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(
        getReaders(10, 10));
    final List<DataTableStub> read = new ArrayList<DataTableStub>();
    for (final DataTableStub record : in) {
      read.add(record);
    }
    in.close();
    assertEquals(20, read.size());
  }

  // Check behavior with no input readers.
  @Test
  public void testWithNoReaders() throws IOException {
    final List<TableReader<DataTableStub>> readers = new ArrayList<TableReader<DataTableStub>>();
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(readers);
    assertFalse(in.iterator().hasNext());
    in.close();
  }

  // Check behavior with empty input readers.
  @Test
  public void testWithEmptyReaders() throws IOException {
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(getReaders(0, 0));
    assertFalse(in.iterator().hasNext());
    in.close();
  }

  // Check that readers are processed in the right order.
  @Test
  public void testReaderOrder() throws IOException {
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(
        getReaders(10, 10, 10, 10, 10));
    final List<DataTableStub> read = new ArrayList<DataTableStub>();
    for (final DataTableStub record : in) {
      read.add(record);
    }
    in.close();
    assertEquals(50, read.size());
    for (int table = 0; table < 5; table++) {
      for (int i = 0; i < 10; i++) {
        final DataTableStub record = read.remove(0);
        assertEquals("t" + table, record.string1);
        assertEquals(i, record.int1.intValue());
      }
    }
  }

  // Check that all iterators are closed after being read
  @Test
  public void testExhaustedReadersClosed() throws IOException {
    final List<TableReader<DataTableStub>> readers = getReaders(10, 10, 10, 10, 10);
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(readers);
    final List<DataTableStub> read = new ArrayList<DataTableStub>();
    for (final DataTableStub record : in) {
      read.add(record);
    }
    for (final TableReader<DataTableStub> reader : readers) {
      assertTrue(((TableReaderStub<DataTableStub>) reader).closed);
    }
    in.close();
  }

  // Check that all iterators are closed after close method called on combined
  // reader.
  @Test
  public void testAllReadersClosed() throws IOException {
    final List<TableReader<DataTableStub>> readers = getReaders(10, 10, 10, 10, 10);
    final TableReader<DataTableStub> in = new CombinedTableReader<DataTableStub>(readers);
    in.close();
    for (final TableReader<DataTableStub> reader : readers) {
      assertTrue(((TableReaderStub<DataTableStub>) reader).closed);
    }
  }
}

class TableReaderStub<T extends DataTable> implements TableReader<T> {

  final TableReaderIteratorStub<T> iterator;
  boolean closed;

  public TableReaderStub(final int items, final String name) {
    this.iterator = new TableReaderIteratorStub<T>(items, name);
    this.closed = false;
  }

  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

}

class TableReaderIteratorStub<T extends DataTable> implements Iterator<T> {

  List<DataTableStub> items;
  int current;

  public TableReaderIteratorStub(final int count, final String name) {
    this.items = new ArrayList<DataTableStub>();
    final TableFormat format = new FormatLibrary()
        .getFormat(Format.DecompressedInternal);
    final DataTableStub base = new DataTableStub(format);
    for (int i = 0; i < count; i++) {
      items.add(new DataTableStub(format, i, name, "123", base.timestamp, base.date));
    }
    this.current = 0;
  }

  @Override
  public boolean hasNext() {
    return current < items.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() {
    return (T) items.get(current++);
  }

  @Override
  public void remove() {
  }

}