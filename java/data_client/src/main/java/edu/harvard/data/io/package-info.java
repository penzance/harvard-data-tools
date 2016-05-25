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
 *
 * While the majority of the classes in this package are based around reading
 * tabular data, the {@link JsonFileReader} enables clients to parse
 * JSON-formatted data. The {@code JsonFileReader} class consumes a data file
 * formatted as newline-delimited JSON objects, and then uses an instance of
 * {@link JsonDocumentParser} to convert those objects to {@code DataTable}
 * records. Clients of the code need to provide a custom
 * {@code JsonDocumentParser}, but do not have to handle the details of parsing
 * JSON.
 */
package edu.harvard.data.io;
