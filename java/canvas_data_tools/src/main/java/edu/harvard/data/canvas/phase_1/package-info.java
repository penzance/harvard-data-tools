/**
 * This package contains verification steps for the identity mapping process.
 * The majority of the identity processing is defined in the common data_client
 * library in a way that is agnostic to the data source. See package
 * {@link edu.harvard.data.identity} for details on the identity map jobs.
 *
 * Verification is split into two passes; the pre-verifier runs before the
 * identity map jobs, and the post-verifier runs after. The pre-verifier is
 * intended to gather any necessary information about the data tables before
 * they are processed, as well as identity information that will be used to
 * replace PII within the tables. The post-verifier checks the output of the
 * identity map jobs to ensure that the correct identifiers were used in cases
 * where a user had already been assigned a research UUID, and that new
 * identifiers were generated in cases where a user had not.
 *
 * In the case of request data, we do not attempt to verify all records. On the
 * occasional instances when Instructure send the entire requests table, this
 * would involve pre-loading hundreds of gigabytes of data into a lookup table,
 * which is clearly impractical. Instead, we sample a subset of request files,
 * and focus on "interesting people" (defined as those who touch a large number
 * of handlers in the sampled files). This gives us a reasonable level of
 * coverage, while keeping the verification process tractable.
 *
 * The work flow for the verification is:
 *
 * <h2>Pre-Verifier</h2>
 * <ol>
 * <li>Compute the set of interesting people for verification, and store to HDFS
 * <li>Fetch Redshift ID information for interesting people, and store to HDFS
 * <li>Spawn a set of pre-verification Hadoop jobs (currently just
 * {@link edu.harvard.data.canvas.phase_1.PreVerifyRequestsJob}) and wait for
 * them to complete.
 * </ol>
 *
 * A pre-verification Hadoop job consists of a mapper that extends
 * {@link edu.harvard.data.canvas.phase_1.PreVerifyMapper} and no reducers. It
 * writes its output to HDFS in the directory specified for phase 1 by
 * {@link edu.harvard.data.DataConfig#getVerifyHdfsDir(int)},
 *
 * <h2>Identity Map and Scrub</h2> See {@link edu.harvard.data.identity} for
 * details.
 *
 * <h2>Post-Verifier</h2>
 * <ol>
 * <li>Read the identity map that was stored on HDFS by the identity job. This
 * is the identity map that will ultimately be written back to Redshift.
 * <li>Iterate through the request data saved by the pre-verifier to replace
 * Canvas Data IDs with research UUIDs. We know that the set of requests is
 * small (since we only sampled a subset of the data in the pre-verifier), so we
 * can run this step inline rather than as a new Hadoop job. On modest AWS
 * hardware, this step runs in under a minute.
 * <li>Spawn a set of post-verification Hadoop jobs (currently
 * {@link edu.harvard.data.canvas.phase_1.PostVerifyRequestMapper}) and wait for
 * the result.
 * </ol>
 *
 * A post-verification Hadoop job reads in whatever saved state is required from
 * HDFS, and then iterates over the full set of records from the dump. It checks
 * each record to see if it was flagged as interesting by the pre-verifier, and
 * if so verifies that the expected research UUIDs are found.
 */
package edu.harvard.data.canvas.phase_1;
