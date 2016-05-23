/**
 * This package contains reader and writer utilities that parse data records
 * from various sources.
 *
 * Data IO is modeled as iterators on top of streams. Since the data sets that
 * will be parsed by this package are likely to be too large to store in memory,
 * iterators allow client code to process the records line-by-line, maintaining
 * a manageable memory footprint.
 *
 * The main interface in this package is {@link TableReader}. This is a marker
 * interface that defines a data source; a {@code TableReader} represents an
 * iterator that will sequentially read the records of a
 * {@link edu.harvard.data.DataTable}. Implementations of this interface handle
 * the details of fetching and reading the data. Clients of this package should
 * be aware that different data sources can generate different exceptions, and
 * so should be aware of the context where these interfaces are used.
 *
 * While there are multiple {@code TableReader} implementations, there is only a
 * single {@link TableWriter}. The {@code TableWriter} outputs {@code DataTable}
 * records to an {@link java.io.OutputStream} or to a local file which, once
 * closed, can be moved elsewhere in the infrastructure by client code.
 */
package edu.harvard.data.io;
