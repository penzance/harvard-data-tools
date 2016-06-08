package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopCacheFileMocker;
import edu.harvard.data.HadoopConfigurationException;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(FileSystem.class)
public class IdentityScrubberTests {

  private static final IdentifierType MAIN_IDENTIFIER = IdentifierType.XID;
  static final URI URI1 = IdentityReducerSetupTests.URI1;
  static final URI URI2 = IdentityReducerSetupTests.URI2;
  static final URI URI3 = IdentityReducerSetupTests.URI3;
  static final String ID_MAP_FILE1 = IdentityReducerSetupTests.ID_MAP_FILE1;
  static final String ID_MAP_FILE2 = IdentityReducerSetupTests.ID_MAP_FILE2;
  static final String ID_MAP_FILE3 = IdentityReducerSetupTests.ID_MAP_FILE3;
  static final String EMPTY_ID_MAP_FILE = IdentityReducerSetupTests.EMPTY_ID_MAP_FILE;

  private Configuration config;
  private Mapper<Object, Text, Text, NullWritable>.Context context;
  private TestIdentityScrubber identityScrubber;
  private FileSystem fs;

  @Before
  @SuppressWarnings("unchecked")
  public void beforeTest() throws IOException, DataConfigurationException {
    config = mock(Configuration.class);
    when(config.get("format")).thenReturn(Format.DecompressedCanvasDataFlatFiles.toString());
    when(config.get("mainIdentifier")).thenReturn(MAIN_IDENTIFIER.toString());
    context = mock(Mapper.Context.class);
    when(context.getConfiguration()).thenReturn(config);
    fs = HadoopCacheFileMocker.setupFilesystem(config);
    when(context.getCacheFiles()).thenReturn(new URI[] { URI1, URI2, URI3 });
    HadoopCacheFileMocker.mockInputStream(fs, URI1, ID_MAP_FILE1);
    HadoopCacheFileMocker.mockInputStream(fs, URI2, ID_MAP_FILE2);
    HadoopCacheFileMocker.mockInputStream(fs, URI3, ID_MAP_FILE3);
    identityScrubber = new TestIdentityScrubber("config_path");

  }

  @Test
  public void noCacheFiles() throws IOException, InterruptedException {
    when(context.getCacheFiles()).thenReturn(new URI[] {});
    identityScrubber.setup(context);
    assertTrue(identityScrubber.identities.isEmpty());
  }

  @Test
  public void emptyCacheFiles() throws IOException, InterruptedException {
    HadoopCacheFileMocker.mockInputStream(fs, URI1, EMPTY_ID_MAP_FILE);
    HadoopCacheFileMocker.mockInputStream(fs, URI2, EMPTY_ID_MAP_FILE);
    HadoopCacheFileMocker.mockInputStream(fs, URI3, EMPTY_ID_MAP_FILE);
    identityScrubber.setup(context);
    assertTrue(identityScrubber.identities.isEmpty());
  }

  @Test
  public void populateMap() throws IOException, InterruptedException {
    identityScrubber.setup(context);
    assertEquals(8, identityScrubber.identities.size());
  }

  @Test
  public void verifyIdentityMap() throws IOException, InterruptedException {
    identityScrubber.setup(context);
    final IdentityMap identityMap = identityScrubber.identities.get("xid5");
    assertNotNull(identityMap);
    assertEquals(555555L, identityMap.get(IdentifierType.CanvasID));
    assertEquals(555550L, identityMap.get(IdentifierType.CanvasDataID));
    assertEquals("huid5", identityMap.get(IdentifierType.HUID));
    assertEquals("xid5", identityMap.get(IdentifierType.XID));
    assertEquals("research_id5", identityMap.get(IdentifierType.ResearchUUID));
  }

  @Test(expected = HadoopConfigurationException.class)
  public void noFormat() throws IOException, InterruptedException {
    when(config.get("format")).thenReturn(null);
    identityScrubber.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void badFormatString() throws IOException, InterruptedException {
    when(config.get("format")).thenReturn("Some unknown format");
    identityScrubber.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void noMainIdentifier() throws IOException, InterruptedException {
    when(config.get("mainIdentifier")).thenReturn(null);
    identityScrubber.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void badMainIdentifier() throws IOException, InterruptedException {
    when(config.get("mainIdentifier")).thenReturn("Some unknown identifier");
    identityScrubber.setup(context);
  }

}

class TestIdentityScrubber extends IdentityScrubber<String> {

  public TestIdentityScrubber(final String configPathString)
      throws IOException, DataConfigurationException {
    super(configPathString);
  }

  @Override
  protected DataTable populateRecord(final CSVRecord csvRecord) {
    return null;
  }

}