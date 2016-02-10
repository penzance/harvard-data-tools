package edu.harvard.data.client.analysis_utils;

import java.util.HashMap;
import java.util.Map;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class UserAgentParser {

  private final UserAgentStringParser parser;
  private final Map<String, ReadableUserAgent> cache;

  public UserAgentParser() {
    this.parser = UADetectorServiceFactory.getResourceModuleParser();
    this.cache = new HashMap<String, ReadableUserAgent>();
  }

  public ReadableUserAgent parse(final String agentString) {
    if (!cache.containsKey(agentString)) {
      cache.put(agentString, parser.parse(agentString));
    }
    return cache.get(agentString);
  }
}
