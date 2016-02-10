package edu.harvard.data.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.harvard.data.client.DataTable;

public class CombinedTableReader<T extends DataTable> implements TableReader<T> {

  private final List<TableReader<T>> tables;
  private final CombinedTableIterator<T> iterator;
  private final Class<T> tableType;

  public CombinedTableReader(final List<TableReader<T>> tables, final Class<T> tableType) {
    this.tables = tables;
    this.tableType = tableType;
    this.iterator = new CombinedTableIterator<T>(tables);
  }

  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    for (final TableReader<T> table : tables) {
      table.close();
    }
  }

  @Override
  public Class<T> getTableType() {
    return tableType;
  }

}

class CombinedTableIterator<T extends DataTable> implements Iterator<T> {

  private Iterator<T> currentIterator;
  private final List<Iterator<T>> iteratorQueue;
  private final List<TableReader<T>> tables;

  public CombinedTableIterator(final List<TableReader<T>> tables) {
    iteratorQueue = new ArrayList<Iterator<T>>();
    this.tables = tables;
    if (tables.size() == 0) {
      currentIterator = null;
    } else {
      currentIterator = tables.get(0).iterator();
    }
    for (int i = 1; i < tables.size(); i++) {
      iteratorQueue.add(tables.get(i).iterator());
    }
  }

  @Override
  public boolean hasNext() {
    if (currentIterator == null) {
      return false;
    }
    if (currentIterator.hasNext()) {
      return true;
    }
    if (iteratorQueue.isEmpty()) {
      closeCurrentIterator();
      return false;
    }
    closeCurrentIterator();
    currentIterator = iteratorQueue.remove(0);
    return hasNext();
  }

  private void closeCurrentIterator() {
    for (final TableReader<T> t : tables) {
      if (t.iterator().equals(currentIterator)) {
        try {
          t.close();
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public T next() {
    return currentIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
