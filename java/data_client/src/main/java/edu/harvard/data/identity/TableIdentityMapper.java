package edu.harvard.data.identity;

import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.generator.IdentityMapperGenerator;

/**
 * Interface to provide table-specific functionality for the Hadoop identity map
 * job. An instance of this interface represents a single row in a data table.
 *
 * There should be an implementation of this interface for every table in a data
 * set that contains identifying information. It is recommended, although not
 * required, that implementations of this interface be generated; see
 * {@link IdentityMapperGenerator} for a generator that matches this interface.
 *
 * @param <T>
 *          the type of the main identifier in the data set. This parameter
 *          should be the same for all tables in a given data set, although it
 *          may vary between data sets.
 */
public interface TableIdentityMapper<T> {

  /**
   * Set up whatever internal data structures will be required to fulfill future
   * method calls on this instance. This method should always be called before
   * {@link #populateIdentityMap} or {@link #readRecord}.
   *
   * @param csvRecord
   *          a {@link CSVRecord} object that contains values for every field in
   *          the table. The individual values may be null, but the
   *          {@code csvRecord} itself may not.
   */
  void readRecord(CSVRecord csvRecord);

  /**
   * Identify columns in the table and the values in the record that contain the
   * main identifier for the data set. A given table may contain more than one
   * primary identifier value (for example, if that table is used to join
   * records for multiple individuals).
   *
   * @return a {@link Map} from identifying column name to the value in this
   *         instance. An entry must appear in this map for every column in the
   *         table that may contain the data set's main identifier, regardless
   *         of whether this particular instance contains a value for that
   *         field. If no identifier is specified for this record, the value in
   *         the map should be null.
   */
  Map<String, T> getMainIdentifiers();

  /**
   * Add all identifiers for this record to the provided {@link IdentityMap}.
   * This method is called only for those tables where there is exactly one main
   * identifier column (as determined by the return value of
   * {@link #getMainIdentifiers()}).
   *
   * This method will not add null values to the identity map. If an identifier
   * field is null in a given record, there will be no entry added to the map.
   *
   * @param id
   *          the identity map instance to populate. It is the responsibility of
   *          the caller to ensure that the {@link IdentifierType#ResearchUUID}
   *          value is added to this map at some point, either before or after
   *          this method call.
   * @return true if any values were added to the identity map, false if not.
   * @throws IdentityImplementationException
   *           if a caller attempts to invoke this method when there are more
   *           than one possible fields that contain the data set's main
   *           identifier.
   */
  boolean populateIdentityMap(IdentityMap id);

}
