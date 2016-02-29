package edu.harvard.data.client.identity;

import java.util.HashMap;
import java.util.Map;

public class IndividualIdentity {

  private final Map<IdentityType, Object> ids;

  public IndividualIdentity() {
    this.ids = new HashMap<IdentityType, Object>();
  }

  public void addIdentity(final IdentityType type, final Object id) {
    if (!id.getClass().isAssignableFrom(type.getDataType())) {
      throw new ClassCastException("Can't assign " + id + " to identity type " + type);
    }
    ids.put(type, id);
  }

  public Map<IdentityType, Object> getIds() {
    return ids;
  }

  public Object getId(final IdentityType type) {
    return ids.get(type);
  }
}
