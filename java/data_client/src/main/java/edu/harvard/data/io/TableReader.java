package edu.harvard.data.io;

import java.io.Closeable;

import edu.harvard.data.DataTable;

/**
 * Marker interface for class that sequentially reads the records of a data set.
 * A {@code TableReader} implements the {@link Iterable} interface, allowing it
 * to be used in an enhanced for loop, and the {@link Closeable} interface which
 * allows compile-time checking that the {@link Closeable#close} method has been
 * called properly.
 *
 *
 */
public interface TableReader<T extends DataTable> extends Iterable<T>, Closeable {
}
