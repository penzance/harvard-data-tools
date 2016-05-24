package edu.harvard.data.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.harvard.data.DataTable;

/**
 * Iterator class that combines the records returned by multiple existing
 * {@link TableReader} instances. The inputs to this iterator can be any
 * {@code TableReader} instances, including other instances of
 * {@code CombinedTableIterator}.
 *
 * The iterator returns all records from all source {@code TableReader}
 * instances in order. It processes each {@code TableReader} in the order in
 * which they were passed to the constructor, and returns the records of each
 * {@code TableReader} in the ordering defined by the {@code TableReader}
 * implementation.
 *
 * The iterator does not cache any records, meaning that its memory footprint is
 * small.
 *
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 *
 * Note that the iteration process can throw an instance of
 * {@link IterationException}. This occurs when an exception is encountered
 * inside the {@link java.util.Iterator#hasNext} or
 * {@link java.util.Iterator#next} methods. Since the {@code Iterator} interface
 * does not declare any checked exceptions, we instead wrap any exceptions in a
 * runtime exception and rely on the calling code to catch them. Client code
 * should also be aware of the specific exception types that may be thrown by
 * each component {@code TableReader}.
 *
 * @param <T>
 *          the {@link DataTable} implementation to be read by this iterator.
 */
class CombinedTableIterator<T extends DataTable> implements Iterator<T> {

  private Iterator<T> currentIterator;
  private final List<Iterator<T>> iteratorQueue;
  private final List<TableReader<T>> tables;

  /**
   * Create an iterator based on an existing set of {@link TableReader} objects.
   * The tables will be processed in the order that they appear in this list.
   *
   * The table readers should all have been initialized, and should not have
   * been closed. Once the records available from a {@code TableReader} instance
   * have been exhausted, the {@code CombinedTableIterator} will call
   * {@link Closeable#close} on that instance.
   *
   * @param tables
   *          a {@link List} of {@code TableReader} instances to be iterated
   *          over.
   */
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