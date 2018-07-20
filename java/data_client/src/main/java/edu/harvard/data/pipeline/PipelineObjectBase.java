package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.elasticmapreduce.model.Configuration;


import edu.harvard.data.DataConfig;

public class PipelineObjectBase {

  protected final DataConfig config;
  protected final List<Field> fields;
  protected String id;
  protected String name;

  protected PipelineObjectBase(final DataConfig config, final String id, final String type) {
    this.config = config;
    this.id = id;
    this.name = id;
    this.fields = new ArrayList<Field>();
    set("type", type);
  }

  void setName(final String name) {
    this.name = name;
  }

  protected void set(final String key, final String value) {
    fields.add(new Field().withKey(key).withStringValue(value));
  }

  protected void set(final String key, final PipelineObjectBase ref) {
    fields.add(new Field().withKey(key).withRefValue(ref.id));
  }

  protected void set(final String key, final Map<String, String> ref) {
	final List<Field> subfields;
	subfields = new ArrayList<Field>();
	for (final String item : ref.keySet()) {
	    subfields.add(new Field().withKey(item).withStringValue( ref.get(item)));
	}
	fields.add(new Field().withKey(key).withStringValue(subfields.toString()));
  }

  protected void set(final String key, final List<Map<String, String>> ref) {
	fields.add(new Field().withKey(key).withStringValue(ref.toString()));
  }
  
  public void setSuccess(final PipelineObjectBase success) {
    set("onSuccess", success);
  }

  void addDependency(final PipelineObjectBase step) {
    set("dependsOn", step);
  }

  protected final PipelineObject getPipelineObject() {
    final PipelineObject obj = new PipelineObject();
    obj.setId(id);
    obj.setName(name);
    obj.setFields(fields);
    return obj;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PipelineObjectBase other = (PipelineObjectBase) obj;
    if (fields == null) {
      if (other.fields != null) {
        return false;
      }
    } else if (!fields.equals(other.fields)) {
      return false;
    }
    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }

}
