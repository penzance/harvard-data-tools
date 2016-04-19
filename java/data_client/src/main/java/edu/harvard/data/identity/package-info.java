/**
 * This package contains the majority of the code needed to manage user
 * identities across data sets.
 * <p>
 * The basic premise behind identity management is that it is undesirable for
 * researchers to automatically have access to semantically-meaningful
 * identifiers for individuals in the data set. While we are not aiming to
 * create a de-identified data set (at least not at this point in the data
 * processing lifecycle), we aim to make it harder for researchers who have not
 * gone through the appropriate authorization process to casually identify
 * individuals.
 * <p>
 * On the other hand, for a data set to be useful for research, we must maintain
 * some concept of identity. Within a single data set, we will often see some
 * user identifier used as a key to link tables. For example, we may have a
 * <code>users</code> table and an <code>events</code> table, where the
 * <code>id</code> column in <code>users</code> is referenced in
 * <code>events</code> as <code>user_id</code>. We need to preserve this
 * relationship in order for the data set to be meaningful.
 * <p>
 * Additionally, we must maintain identity relationships between data sets. Just
 * as user identifiers are necessary when joining tables within a data set, it
 * is important that we be able to join disparate data sets that share a common
 * user identifier, even though we do not want to reveal the true value of that
 * identifier for any individual user.
 * <p>
 * A final requirement is that, although identities should be obscured in the
 * general case, we must maintain the ability to re-identify users if necessary.
 * The process by which a researcher is authorized to see real user identities
 * is outside the scope of the system, but there are legitimate cases where a
 * researcher may wish to join the data produced by this system with some
 * external data source.
 * <p>
 * For a given data set, we require a JSON document that describes the tables
 * and columns that contain user identifiers. See
 * {@link IdentitySchemaTransformer} for a description of the JSON
 * specification. We also require that each data set defines a
 * <code>main identifier</code> value. This is an instance of the
 * {@code IdentifierType} enum that represents the primary means of identifying
 * a user in that data set. This is generally the primary key of the
 * <code>user</code> table, or some similar identifier.
 * <p>
 * We define a single <code>research UUID</code> that represents a user across
 * data sets. This ID is semantically-meaningless; it is a randomly-generated
 * string that does not act as a key into any external system. However, it is
 * guaranteed to be unique to the user it represents, and to consistently
 * represent that user across data sets. The research UUID is the primary key of
 * the <code>identity_map</code> database table, which allows us to reconstruct
 * a user's identity if necessary. We assume that access to the
 * <code>identity_map</code> table is closely guarded.
 * <p>
 * The basic approach is that we set aside a phase in the data processing
 * pipeline (generally Phase 1) to handle identity. During this phase we perform
 * two sets of map reduce jobs:
 * <p>
 * <b>Identity Job</b>. There is one identity job for a given data set. It uses
 * one mapper per identifying table, and a single reducer. Each mapper produces
 * a map from the main identifier type to a collection of
 * {@link HadoopIdentityKey} values that contain all identifiers for that user
 * found in the table. The job then runs a single reducer that gathers all
 * {@code HadoopIdentityKey} values for a given main identifier and produces a
 * single {@link IdentityMap} object for that user.
 * <p>
 * <b>Scrubber Job</b>. The scrubber jobs run after the identity job is
 * complete. There is one scrubber job per identifying table, each of which
 * contains one mapper and no reducer. The scrubber mapper takes the identity
 * map generated in the identity job and processes every line of the input
 * table. It sets the research UUID for any users that appear in the original
 * row, and strips out any other identities.
 * <p>
 * In order to ensure that research UUIDs remain consistent between multiple
 * data sets (and multiple processing runs of the same data set), we require
 * that the current identity_map table be supplied as an input to the identity
 * job. That way, if the identity reducer encounters a user for whom a research
 * UUID has already been generated (this is the common case), it can re-use that
 * ID rather than generating a new one. To avoid race conditions in updating the
 * identity_map table, we require that only one data pipeline run its identity
 * processing phase at a time, and that the updated identity_map be written back
 * to the database before the next pipeline begins to process identities. Access
 * to the identity_map is controlled by a lease in DynamoDB (see the
 * {@link edu.harvard.data.leases} package for details on leases).
 * <p>
 * It is recommended (although not requred) that the per-table mappers and
 * scrubbers be generated. See
 * {@link edu.harvard.data.generator.IdentityMapperGenerator} and
 * {@link edu.harvard.data.generator.IdentityScrubberGenerator} for generator
 * code for these two sets of classes. To simplify any generated code, this
 * package contains base implementations of both jobs that can be extended with
 * minimal additional code. Because Hadoop requires that the types of keys and
 * values be declared as part of a job's definition we have multiple base
 * classes for the mappers and scrubbers, to be used depending on the type of
 * the main identifier. See {@link LongIdentityMapper} and
 * {@link LongIdentityScrubber} for examples.
 */
package edu.harvard.data.identity;
