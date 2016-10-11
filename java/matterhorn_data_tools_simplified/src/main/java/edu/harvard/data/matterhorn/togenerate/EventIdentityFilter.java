package edu.harvard.data.matterhorn.togenerate;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataTable;
import edu.harvard.data.DataFilter;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.matterhorn.bindings.deidentified.DeidentifiedEvent;
import edu.harvard.data.matterhorn.bindings.input.InputEvent;

public class EventIdentityFilter implements DataFilter {

  private final IdentityService idService;
  private final DataConfig config;

  public EventIdentityFilter(final IdentityService idService, final DataConfig config) {
    this.idService = idService;
    this.config = config;
  }

  @Override
  public DataTable filter(final DataTable record) {
    final InputEvent in = (InputEvent)record;
    final IdentityMap id = new IdentityMap();
    id.set(IdentifierType.HUID, in.getHuid());
    final String researchUuid = idService.getResearchUuid(id, config.getMainIdentifier());
    final DeidentifiedEvent filtered = new DeidentifiedEvent(in);
    filtered.setHuidResearchUuid(researchUuid);
    return filtered;
  }
}
