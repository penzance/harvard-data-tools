package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopConfigurationException;

public class IdentityMapperTests {

  private Configuration config;
  private Mapper<Object, Text, LongWritable, HadoopIdentityKey>.Context context;

  @Before
  @SuppressWarnings("unchecked")
  public void beforeTest() throws IOException {
    config = mock(Configuration.class);
    when(config.get("format")).thenReturn(Format.DecompressedInternal.toString());
    context = mock(Mapper.Context.class);
    when(context.getConfiguration()).thenReturn(config);
  }

  private List<IdentityMap> getWrittenMaps() throws IOException, InterruptedException {
    final ArgumentCaptor<LongWritable> key = ArgumentCaptor.forClass(LongWritable.class);
    final ArgumentCaptor<HadoopIdentityKey> value = ArgumentCaptor
        .forClass(HadoopIdentityKey.class);
    verify(context).write(key.capture(), value.capture());
    final List<IdentityMap> maps = new ArrayList<IdentityMap>();
    for (final HadoopIdentityKey hadoopIdentityKey : value.getAllValues()) {
      maps.add(hadoopIdentityKey.getIdentityMap());
    }
    return maps;
  }

  private List<Long> getWrittenKeys(final int times) throws IOException, InterruptedException {
    final ArgumentCaptor<LongWritable> key = ArgumentCaptor.forClass(LongWritable.class);
    final ArgumentCaptor<HadoopIdentityKey> value = ArgumentCaptor
        .forClass(HadoopIdentityKey.class);
    verify(context, times(times)).write(key.capture(), value.capture());
    final List<Long> keys = new ArrayList<Long>();
    for (final LongWritable longWritable : key.getAllValues()) {
      keys.add(longWritable.get());
    }
    return keys;
  }

  @Test(expected = HadoopConfigurationException.class)
  public void noFormat() throws IOException, InterruptedException {
    final LongIdentityMapper mapper = new TestIdentityMapper();
    when(config.get("format")).thenReturn(null);
    mapper.setup(context);
  }

  @Test(expected = HadoopConfigurationException.class)
  public void badFormatString() throws IOException, InterruptedException {
    final LongIdentityMapper mapper = new TestIdentityMapper();
    when(config.get("format")).thenReturn("Some unknown format");
    mapper.setup(context);
  }

  @Test
  // Reduce method with no keys. Check that readRecord was called.
  public void noKeys() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.map(null, new Text("value"), context);
    assertNotNull(mapper.passedRecord);
  }

  @Test
  // Reduce with one key. Check that populate was called with the right value.
  public void oneKeyPopulate() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", 123L);
    mapper.map(null, new Text("value"), context);
    assertEquals(1, mapper.passedIds.size());
    assertEquals(new IdentityMap(), mapper.passedIds.get(0));
  }

  @Test
  // Reduce with one key. Context written if populateIdentityMap returned true
  public void oneKeyContextWritten() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", 123L);
    mapper.setIdentityMapValues("ResearchID", "HUID", "XID", 123L, 456L);
    mapper.map(null, new Text("value"), context);
    final IdentityMap written = getWrittenMaps().get(0);
    assertEquals("ResearchID", written.get(IdentifierType.ResearchUUID));
    assertEquals("HUID", written.get(IdentifierType.HUID));
    assertEquals("XID", written.get(IdentifierType.XID));
    assertEquals(123L, written.get(IdentifierType.CanvasID));
    assertEquals(456L, written.get(IdentifierType.CanvasDataID));
  }

  @Test
  // Reduce with one key. Context not written if populate returned false
  public void oneKeyContextNotWritten() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", 123L);
    mapper.populated = false;
    mapper.map(null, new Text("value"), context);
    assertEquals(1, mapper.passedIds.size());
    verify(context, never()).write((LongWritable) any(), (HadoopIdentityKey) any());
  }

  @Test
  // Reduce with two keys, both null. Check write never called
  public void twoKeysNull() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", (Long) null);
    mapper.addKey("Key2", (Long) null);
    mapper.populated = false;
    mapper.map(null, new Text("value"), context);
    verify(context, never()).write((LongWritable) any(), (HadoopIdentityKey) any());
  }

  @Test
  // Reduce with two keys, one null.
  public void twoKeysOneNull() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", 123L);
    mapper.addKey("Key2", (Long) null);
    mapper.populated = false;
    mapper.map(null, new Text("value"), context);
    assertEquals((Long) 123L, getWrittenKeys(1).get(0));
  }

  @Test
  // Reduce with two keys, neither null.
  public void twoKeysNoneNull() throws IOException, InterruptedException {
    final TestIdentityMapper mapper = new TestIdentityMapper();
    mapper.addKey("Key1", 123L);
    mapper.addKey("Key2", 456L);
    mapper.populated = false;
    mapper.map(null, new Text("value"), context);
    final List<Long> writtenKeys = getWrittenKeys(2);
    assertTrue(writtenKeys.contains(123L));
    assertTrue(writtenKeys.contains(456L));
  }
}

class TestIdentityMapper extends LongIdentityMapper {

  Map<String, Long> keys;
  boolean populated;
  CSVRecord passedRecord;
  List<IdentityMap> passedIds;
  List<IdentityMap> idValues;

  public TestIdentityMapper() {
    this.keys = new HashMap<String, Long>();
    this.populated = true;
    this.passedIds = new ArrayList<IdentityMap>();
    this.idValues = new ArrayList<IdentityMap>();
    this.mapper = new IdentityMapper<Long>();
    this.mapper.format = new FormatLibrary().getFormat(Format.DecompressedInternal);

  }

  public void addKey(final String key, final Long value) {
    keys.put(key, value);
  }

  public void setIdentityMapValues(final String rid, final String huid, final String xid,
      final Long canvasId, final Long canvasDataId) {
    final IdentityMap id = new IdentityMap();
    id.set(IdentifierType.ResearchUUID, rid);
    id.set(IdentifierType.HUID, huid);
    id.set(IdentifierType.XID, xid);
    id.set(IdentifierType.CanvasID, canvasId);
    id.set(IdentifierType.CanvasDataID, canvasDataId);
    idValues.add(id);
  }

  @Override
  public void readRecord(final CSVRecord csvRecord) {
    passedRecord = csvRecord;
  }

  @Override
  public Map<String, Long> getMainIdentifiers() {
    return keys;
  }

  @Override
  public boolean populateIdentityMap(final IdentityMap id) {
    if (!idValues.isEmpty()) {
      final IdentityMap values = idValues.remove(0);
      for (final IdentifierType type : IdentifierType.values()) {
        if (type != IdentifierType.Other) {
          id.set(type, values.get(type));
        }
      }
    }
    passedIds.add(id);
    return populated;
  }

}