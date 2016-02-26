package edu.harvard.data.client.identity;

import java.util.HashMap;
import java.util.Map;

public class IndividualIdentity {

  private final Map<IdentityType, String> ids;

  public IndividualIdentity() {
    this.ids = new HashMap<IdentityType, String>();
  }

  public void addIdentity(final IdentityType type, final String id) {
    ids.put(type, id);
  }

  public Map<IdentityType, String> getIds() {
    return ids;
  }

  public String getId(final IdentityType type) {
    return ids.get(type);
  }
}
