package edu.harvard.data.identity;

import java.util.List;

import edu.harvard.data.DataConfig;

public class HuidEppnLookup {
  private static final int LOOKUP_CHUNK_SIZE = 2;
  private final DataConfig config;

  public HuidEppnLookup(final DataConfig config) {
    this.config = config;
  }

  public void expandIdentities(final List<IdentityMap> identities) {
    for (int i=0; i<identities.size(); i+= LOOKUP_CHUNK_SIZE) {
      final int start = i;
      final int end = Math.min(start + LOOKUP_CHUNK_SIZE, identities.size());
      expandIdentities(identities, start, end);
    }
  }

  private void expandIdentities(final List<IdentityMap> identities, final int start, final int end) {
    String queryString = "SELECT * FROM " + " WHERE huid IN (";
    for (int i = start; i < end; i++) {
      queryString += "?, ";
    }
    queryString = queryString.substring(0, queryString.length() - 2) + ");";
    System.out.println(queryString);

    for (int i = start; i < end; i++) {
      System.out.println("Setting parameter to " + identities.get(i).get(IdentifierType.HUID));
    }
  }
}
