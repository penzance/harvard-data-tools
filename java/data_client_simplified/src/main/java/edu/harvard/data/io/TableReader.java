package edu.harvard.data.io;

import java.io.Closeable;

import edu.harvard.data.DataTable;

/**
 * Marker interface for class that sequentially reads the records of a data set.
 * A {@code TableReader} implements the {@link Iterable} interface, allowing it
 * to be used in an enhanced for loop, and the {@link Closeable} interface which
 * enables compile-time checking that the {@link Closeable#close} method has
 * been called properly, and to allow the {@code TableReader} to be used in a
 * try-with-resources construct.
 * <P>
 * A typical pattern to iterate through the records of a {@code TableReader}
 * would be:
 * <pre>
 * {@code
 * try (
 *   TableReader<IdentityMap> in = getTableReader(context, format, IdentityMap.class)
 * ) {
 *   for (final IdentityMap id : in) {
 *     identities.put((T) id.get(mainIdentifier), id);
 *   }
 * } catch (final IterationException ie) {
 *   final Throwable e = ie.getCause();
 *   // Exception handler code.
 * } }
 * </pre>
 * Note that the iteration process can throw an instance of
 * {@link IterationException}. This occurs when an exception is encountered
 * inside the {@link java.util.Iterator#hasNext} or
 * {@link java.util.Iterator#next} methods. Since the {@code Iterator} interface
 * does not declare any checked exceptions, we instead wrap any exceptions in a
 * runtime exception and rely on the calling code to catch them.
 */
public interface TableReader<T extends DataTable> extends Iterable<T>, Closeable {
}
