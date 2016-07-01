package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopCacheFileMocker;
import edu.harvard.data.HadoopConfigurationException;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(FileSystem.class)
public class IdentityReducerSetupTests {

  static final IdentifierType MAIN_IDENTIFIER = IdentifierType.XID;
  static final URI URI1 = URI.create("mock://cache_file_1");
  static final URI URI2 = URI.create("mock://cache_file_2");
  static final URI URI3 = URI.create("mock://cache_file_3");
  static final String ID_MAP_FILE1 = "identity_map_data/identity_map_1.txt";
  static final String ID_MAP_FILE2 = "identity_map_data/identity_map_2.txt";
  static final String ID_MAP_FILE3 = "identity_map_data/identity_map_3.txt";
  static final String EMPTY_ID_MAP_FILE = "identity_map_data/empty_identity_map.txt";

  private Configuration config;
  private Reducer<?, HadoopIdentityKey, Text, NullWritable>.Context context;
  private FileSystem fs;
  private IdentityReducer<String> identityReducer;

  @Before
  @SuppressWarnings("unchecked")
  public void beforeTest() throws IOException {
    config = mock(Configuration.class);
    when(config.get("format")).thenReturn(Format.DecompressedInternal.toString());
    when(config.get("mainIdentifier")).thenReturn(MAIN_IDENTIFIER.toString());
    context = mock(Reducer.Context.class);
    when(context.getConfiguration()).thenReturn(config);
    fs = HadoopCacheFileMocker.setupFilesystem(config);
    when(context.getCacheFiles()).thenReturn(new URI[] { URI1, URI2, URI3 });
    HadoopCacheFileMocker.mockInputStream(fs, URI1, ID_MAP_FILE1);
    HadoopCacheFileMocker.mockInputStream(fs, URI2, ID_MAP_FILE2);
    HadoopCacheFileMocker.mockInputStream(fs, URI3, ID_MAP_FILE3);
    identityReducer = new IdentityReducer<String>();
  }

  @Test
  public void noCacheFiles() throws IOException {
    when(context.getCacheFiles()).thenReturn(new URI[] {});
    identityReducer.setup(context);
    assertTrue(identityReducer.identities.isEmpty());
  }

  @Test
  public void emptyCacheFiles() throws IOException {
    HadoopCacheFileMocker.mockInputStream(fs, URI1, EMPTY_ID_MAP_FILE);
    HadoopCacheFileMocker.mockInputStream(fs, URI2, EMPTY_ID_MAP_FILE);
    HadoopCacheFileMocker.mockInputStream(fs, URI3, EMPTY_ID_MAP_FILE);
    identityReducer.setup(context);
    assertTrue(identityReducer.identities.isEmpty());
  }

  @Test
  public void populateMap() throws IOException {
    identityReducer.setup(context);
    assertEquals(8, identityReducer.identities.size());
  }

  @Test
  public void verifyIdentityMap() throws IOException {
    identityReducer.setup(context);
    final IdentityMap identityMap = identityReducer.identities.get("xid5");
    assertNotNull(identityMap);
    assertEquals(555555L, identityMap.get(IdentifierType.CanvasID));
    assertEquals(555550L, identityMap.get(IdentifierType.CanvasDataID));
    assertEquals("huid5", identityMap.get(IdentifierType.HUID));
    assertEquals("xid5", identityMap.get(IdentifierType.XID));
    assertEquals("research_id5", identityMap.get(IdentifierType.ResearchUUID));
  }

  @Test(expected = HadoopConfigurationException.class)
  public void noFormat() throws IOException {
    when(config.get("format")).thenReturn(null);
    identityReducer.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void badFormatString() throws IOException {
    when(config.get("format")).thenReturn("Some unknown format");
    identityReducer.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void noMainIdentifier() throws IOException {
    when(config.get("mainIdentifier")).thenReturn(null);
    identityReducer.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void badMainIdentifier() throws IOException {
    when(config.get("mainIdentifier")).thenReturn("Some unknown identifier");
    identityReducer.setup(context);
  }

}
