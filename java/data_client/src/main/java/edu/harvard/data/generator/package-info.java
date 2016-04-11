/**
 * This package contains the code generator that builds Java classes, as well as
 * Bash and SQL scripts that correspond to a dataset schema. The code in this
 * package is explicitly not tied to any particular data set. The intent is that
 * any dataset-specific code is factored out to a dataset-specific caller.
 * <P>
 * The main interface to this package is the
 * {@link edu.harvard.data.generator.GenerationSpec} class. A client of this
 * code should populate a {@code SchemaTransformer} with
 * {@link edu.harvard.data.schema.DataSchema} instances representing each
 * required processing phase, along with the additional data such as Java
 * packages or class prefixes that will be required to generate a standard set
 * of files. See the documentation for {@code GenerationSpec} for details of the
 * parameters that are required.
 * <P>
 * Once the caller has build the {@code GenerationSpec} object, the various
 * generators can then be invoked. See the documentation for each generator for
 * the files that it creates. By convention generators in this package have the
 * suffix "Generator".
 * <P>
 * The Java code generator is more complex than the generators for other
 * scripts, and so is split into multiple generator classes. The entry point to
 * the Java generator is {@link edu.harvard.data.generator.JavaBindingGenerator}.
 */
package edu.harvard.data.generator;
