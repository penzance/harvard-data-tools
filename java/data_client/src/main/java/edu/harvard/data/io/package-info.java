/**
 * This package contains reader and writer utilities that parse data records
 * from various sources.
 * 
 * Data IO is modeled as iterators on top of streams. Since the data sets that
 * will be parsed by this package are likely to be too large to store in memory,
 * iterators allow client code to process the records line-by-line, maintaining
 * a manageable memory footprint.
 * 
 * There are two main interfaces in the package: {@link TableReader} and
 * {@link TableWriter}, along with various implementations for different data
 * sources. Clients of this package should be aware that different data sources
 * can generate different exceptions, and so should be aware of the context
 * where these interfaces are used.
 */
package edu.harvard.data.io;
