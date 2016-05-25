package edu.harvard.data.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import edu.harvard.data.DataTable;

/**
 * Implementation of the {@link TableReader} interface that iterates over the
 * records returned by multiple other table readers. This class exists as a
 * convenience to simplify the case where a data table is represented by
 * multiple files.
 * <P>
 * This class is a simple wrapper around the {@link CombinedTableIterator} type;
 * see the documentation for that class for more details.
 *
 * @param <T>
 *          the record type that this reader parses.
 */
public class CombinedTableReader<T extends DataTable> implements TableReader<T> {

  private final List<TableReader<T>> tables;
  private final CombinedTableIterator<T> iterator;

  /**
   * Create a combined reader from a list of existing {@link TableReader}
   * instances. The iterator returned by this instance will return all the
   * records in each table reader in the order that they are specified in the
   * {@code List}.
   *
   * @param tables
   *          an ordered {@link List} of {@code TableReader} instances that will
   *          provide the records for this iterator.
   */
  public CombinedTableReader(final List<TableReader<T>> tables) {
    this.tables = tables;
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

}
