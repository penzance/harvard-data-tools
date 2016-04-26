package edu.harvard.data.identity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wrapper class to allow Hadoop to serialize an {@link IdentityMap}. This class
 * simply converts the {@code IdentityMap} instance to a JSON representation
 * which can then be written to HDFS.
 */
public class HadoopIdentityKey implements WritableComparable<HadoopIdentityKey> {

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private IdentityMap id;

  public HadoopIdentityKey() {
  }

  public HadoopIdentityKey(final IdentityMap id) {
    this.id = id;
  }

  public void setId(final IdentityMap id) {
    this.id = id;
  }

  public IdentityMap getIdentityMap() {
    return id;
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    final String json = jsonMapper.writeValueAsString(id);
    out.writeUTF(json);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final String json = in.readUTF();
    this.id = jsonMapper.readValue(json, IdentityMap.class);
  }

  @Override
  public int compareTo(final HadoopIdentityKey o) {
    return id.compareTo(o.id);
  }

}
