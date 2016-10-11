package edu.harvard.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class CloseableMap<K, V extends Closeable> implements Closeable {

  private final Map<K, V> map;

  public CloseableMap(final Map<K, V> map) {
    this.map = map;
  }

  public Map<K, V> getMap() {
    return map;
  }

  @Override
  public void close() throws IOException {
    for(final Closeable item : map.values()) {
      item.close();
    }
  }

}
