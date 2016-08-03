package edu.harvard.data.identity;

import java.util.regex.Pattern;

/**
 * Enumeration of the different identifiers that appear across the various data
 * sets. The identifiers in this enum may or may not appear in any given data
 * set; since the <code>identity_map</code> table maintains a mapping from
 * <code>research_id</code> to every other identifier belonging to an
 * individual, we need to maintain a global view of identity. Thus, the
 * identifier type is part of the common cross-data set code base, rather than
 * factored out to the individual implementations.
 *
 * There are two special identifiers in the enumeration. First, the
 * {@code ResearchUUID} acts as the key into the <code>identity_map</code>
 * table, and so must be present in all data sets. Second, the {@code Other}
 * value does not appear in the <code>identity_map</code> table. It is used in
 * the data set identifier JSON specification to indicate a field that contains
 * identifying information that should not be retained in the data set. For
 * example, e-mail correspondence between student and instructor will generally
 * be flagged as {@code Other}, and will be automatically stripped out during
 * the identity map reduce jobs.
 *
 * Each enum value declares the type of the identifier. This knowledge is used
 * to parameterize the identity map reduce jobs (see the descriptions of
 * {@link IdentityMapper} and {@link LongIdentityMapper} for a discussion of the
 * need for identifier types). For identifiers typed as {@code String}, we also
 * declare a regular expression pattern that can differentiate between values
 * multiplexed in a single column.
 */
public enum IdentifierType {
  ResearchUUID("research_id",
      "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}", String.class),

  HUID("huid", "\\d{8}", String.class),

  XID("xid", "[a-zA-Z][\\d|\\w]{7}", String.class),

  CanvasID("canvas_id", "", Long.class),

  CanvasDataID("canvas_data_id", "", Long.class),

  EmailAddress("email",
      "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$",
      String.class),

  Name("name", "", String.class),

  Other("other", "", Void.class);

  private String fieldName;
  private Pattern pattern;
  private Class<?> type;

  private IdentifierType(final String fieldName, final String regex, final Class<?> type) {
    this.fieldName = fieldName;
    pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    this.type = type;
  }

  /**
   * Get the name by which this identifier is referred in the
   * <code>identity_map</code> database table.
   *
   * @return a {@code String} label for this enum value that is guaranteed to be
   *         unique among other values of the enumeration. This label should be
   *         used when accessing the <code>identity_map</code> database table.
   *
   * @throws IdentityImplementationException
   *           if {@code this} value is {@code Other}.
   */
  public String getFieldName() {
    if (this.equals(Other)) {
      throw new IdentityImplementationException("Can't access field name for identifier Other.");
    }
    return fieldName;
  }

  public static IdentifierType fromFieldName(final String fieldName) {
    for (final IdentifierType id : values()) {
      if (id.getFieldName().equals(fieldName)) {
        return id;
      }
    }
    return valueOf(fieldName);
  }


  /**
   * Get a regular expression pattern by which this String-typed enumeration
   * value can be identified.
   *
   * @return a {@link Pattern} object that can be matched against a
   *         {@code String} to determine whether that {@code String} contains an
   *         identifier of this type.
   *
   * @throws IdentityImplementationException
   *           if {@code this} value is not typed as a {@code String}.
   */
  public Pattern getPattern() {
    if (!type.equals(String.class)) {
      throw new IdentityImplementationException(
          "Can't access regex pattern for an identifier of type " + type);
    }
    return pattern;
  }

  /**
   * Get the Java type of this identifier.
   *
   * @return the {@code Class} object that represents the type of values of this
   *         identifier.
   */
  public Class<?> getType() {
    return type;
  }
}
