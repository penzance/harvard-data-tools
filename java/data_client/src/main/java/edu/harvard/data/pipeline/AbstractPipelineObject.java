package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;

import edu.harvard.data.DataConfigurationException;

public abstract class AbstractPipelineObject {

  protected final DataConfig params;
  protected final List<Field> fields;
  protected String id;
  protected String name;
  protected final Set<AbstractPipelineObject> children;
  protected boolean finalized;
  protected String type;

  protected AbstractPipelineObject(final DataConfig params, final String id, final String type) {
    this.params = params;
    this.id = id;
    this.name = id;
    this.children = new HashSet<AbstractPipelineObject>();
    this.fields = new ArrayList<Field>();
    this.finalized = false;
    this.type = type;
    set("type", type);
  }

  void setName(final String name) {
    this.name = name;
  }

  protected void set(final String key, final String value) {
    fields.add(new Field().withKey(key).withStringValue(value));
  }

  protected void set(final String key, final AbstractPipelineObject ref) {
    fields.add(new Field().withKey(key).withRefValue(ref.id));
    children.add(ref);
  }

  protected Set<AbstractPipelineObject> getChildren() {
    final Set<AbstractPipelineObject> allChildren = new HashSet<AbstractPipelineObject>(children);
    for (final AbstractPipelineObject child : children) {
      allChildren.addAll(child.getChildren());
    }
    return allChildren;
  }

  protected final PipelineObject getPipelineObject() throws DataConfigurationException {
    final PipelineObject obj = new PipelineObject();
    obj.setId(id);
    obj.setName(name);
    obj.setFields(fields);
    return obj;
  }
}
