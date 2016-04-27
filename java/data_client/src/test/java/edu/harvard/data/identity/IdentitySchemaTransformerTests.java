package edu.harvard.data.identity;

public class IdentitySchemaTransformerTests {
  // Check that the original schema isn't modified.
  // Check that all original tables are still present.
  // Check that no new tables have been added.
  // Check that identifier columns have been removed.
  // Check that a research UUID column is present for all non-Other identifiers.
  // Check that no research UUID column has been generated for an Other column.
  // Attempt to transform table that is not in the original schema.
  // Attempt to transform column that is not in the original schema.
}
