package edu.harvard.data.canvas.steps;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.harvard.data.CloseableMap;
import edu.harvard.data.DataTable;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.canvas.bindings.phase_1.Phase1Requests;
import edu.harvard.data.canvas.bindings.phase_2.Phase2Requests;
import edu.harvard.data.io.TableWriter;
import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.OperatingSystemFamily;
import net.sf.uadetector.ReadableDeviceCategory;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentFamily;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.UserAgentType;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class Phase2RequestsStep implements ProcessingStep {

  private final UserAgentStringParser parser;
  private final Map<String, ReadableUserAgent> cache;
  private final Pattern iosPattern;
  private final Pattern androidPattern;

  public Phase2RequestsStep() {
    this.parser = UADetectorServiceFactory.getResourceModuleParser();
    this.cache = new HashMap<String, ReadableUserAgent>();
    this.iosPattern = Pattern.compile("(\\w+)/(\\w+) \\((.*);(.*);(.*)\\)");
    this.androidPattern = Pattern.compile("candroid_\\(w+)");
  }

  @Override
  public DataTable process(final DataTable record,
      final CloseableMap<String, TableWriter<DataTable>> extraOutputs) throws IOException {
    final Phase1Requests in = (Phase1Requests) record;
    final Phase2Requests out = new Phase2Requests(in);
    parseUserAgent(out);
    parseMobile(out);
    return out;
  }

  private void parseMobile(final Phase2Requests out) {
    out.setMobile("iOS".equals(out.getOs()) || "Android".equals(out.getBrowser()));
  }

  private void parseUserAgent(final Phase2Requests out) {
    final String agentString = out.getUserAgent();
    if (agentString != null) {
      final ReadableUserAgent agent = parse(agentString);
      out.setBrowser(agent.getName());
      out.setOs(agent.getOperatingSystem().getName());
    }
  }

  // Mobile user agents (per Canvas development team)
  // iOS: <App Name>/<App Version> (<DeviceName>/<Device>; <Device OS>; <Device
  // Scale>)
  // example: iCanvas/141 (iPhone; iOS 8.2; Scale/2.00)
  //
  // Android: candroid_versionnumber
  // example: candroid_1234
  private ReadableUserAgent parse(final String agentString) {
    if (!cache.containsKey(agentString)) {
      ReadableUserAgent ua = parser.parse(agentString);
      if (ua == null) {
        final Matcher iosMatcher = iosPattern.matcher(agentString);
        if (iosMatcher.matches() && iosMatcher.group(4).toLowerCase().contains("ios")) {
          ua = new MobileUserAgent(iosMatcher.group(1), OperatingSystemFamily.IOS, "iOS",
              new VersionNumber(iosMatcher.group(2)));
        } else {
          final Matcher androidMatcher = androidPattern.matcher(agentString);
          if (androidMatcher.matches()) {
            ua = new MobileUserAgent("candroid", OperatingSystemFamily.ANDROID, "Android",
                new VersionNumber(androidMatcher.group(1)));
          }
        }
      }
      synchronized (cache) {
        cache.put(agentString, ua);
      }
    }
    return cache.get(agentString);
  }

  @Override
  public Void call() throws Exception {
    return null;
  }
}

class MobileUserAgent implements ReadableUserAgent {

  private final String name;
  private final OperatingSystem os;

  public MobileUserAgent(final String browser, final OperatingSystemFamily osFamily,
      final String osName, final VersionNumber osVersion) {
    this.name = browser;
    this.os = new OperatingSystem(osFamily, "", "", osName, "", "", "", osVersion);
  }

  @Override
  public ReadableDeviceCategory getDeviceCategory() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UserAgentFamily getFamily() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getIcon() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public OperatingSystem getOperatingSystem() {
    return os;
  }

  @Override
  public String getProducer() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getProducerUrl() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UserAgentType getType() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getTypeName() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String getUrl() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public VersionNumber getVersionNumber() {
    throw new RuntimeException("Not implemented");
  }
}
