package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;

public class IdentityReducerReduceTests {

  private static final IdentifierType MAIN_IDENTIFIER = IdentifierType.XID;
  private static final String XID = "known_xid";

  private Configuration config;
  private Reducer<Text, HadoopIdentityKey, Text, NullWritable>.Context context;
  private IdentityReducer<String> identityReducer;

  @Before
  @SuppressWarnings("unchecked")
  public void beforeTest() throws IOException {
    config = mock(Configuration.class);
    context = mock(Reducer.Context.class);
    when(context.getConfiguration()).thenReturn(config);
    identityReducer = new IdentityReducer<String>(MAIN_IDENTIFIER);
    identityReducer.format = new FormatLibrary().getFormat(Format.DecompressedCanvasDataFlatFiles);
  }

  private Iterable<HadoopIdentityKey> getIterable(final IdentityMap... identityMap) {
    final List<HadoopIdentityKey> lst = new ArrayList<HadoopIdentityKey>();
    for (final IdentityMap key : identityMap) {
      lst.add(new HadoopIdentityKey(key));
    }
    return lst;
  }

  private IdentityMap makeId() {
    final IdentityMap id = new IdentityMap();
    id.set(IdentifierType.ResearchUUID, "Research ID");
    id.set(IdentifierType.HUID, "HUID");
    id.set(IdentifierType.XID, XID);
    id.set(IdentifierType.CanvasID, 12345);
    id.set(IdentifierType.CanvasDataID, 67890);
    return id;
  }

  private IdentityMap makeId(final String xid) {
    final IdentityMap id = new IdentityMap();
    id.set(IdentifierType.XID, xid);
    return id;
  }

  private IdentityMap getWrittenMap() throws IOException, InterruptedException {
    final ArgumentCaptor<Text> key = ArgumentCaptor.forClass(Text.class);
    final ArgumentCaptor<NullWritable> value = ArgumentCaptor.forClass(NullWritable.class);
    verify(context).write(key.capture(), value.capture());
    final String[] values = key.getValue().toString().split("\t");
    final IdentityMap id = new IdentityMap();
    id.set(IdentifierType.ResearchUUID, values[0]);
    id.set(IdentifierType.HUID, values[1]);
    id.set(IdentifierType.XID, values[2]);
    id.set(IdentifierType.CanvasID, values[3].equals("\\N") ? null : Long.parseLong(values[3]));
    id.set(IdentifierType.CanvasDataID, values[4].equals("\\N") ? null : Long.parseLong(values[4]));
    return id;
  }

  @Test
  public void existingUUID() throws IOException, InterruptedException {
    final IdentityMap id = makeId();
    identityReducer.identities.put(XID, id);
    identityReducer.reduce(XID, getIterable(makeId(XID)), context);
    final IdentityMap written = getWrittenMap();
    assertEquals(id.get(IdentifierType.ResearchUUID), written.get(IdentifierType.ResearchUUID));
  }

  @Test
  public void newUUID() throws IOException, InterruptedException {
    final IdentityMap id = makeId();
    id.set(IdentifierType.XID, "some_other_xid");
    identityReducer.identities.put(XID, id);
    identityReducer.reduce(XID, getIterable(makeId(XID)), context);
    final IdentityMap written = getWrittenMap();
    assertNotEquals(id.get(IdentifierType.ResearchUUID), written.get(IdentifierType.ResearchUUID));
  }



  // Check existing object uses the right UUID
  // Check new identity generates a new UUID
  // Check that new identity data is properly added
  // Give a list of some nulls and some populated. Check they are collapsed
  // Empty values?
  // Test with empty identity map
  //
}
