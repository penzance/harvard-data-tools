package edu.harvard.data.canvas.togenerate;

import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.canvas.identity.CanvasIdentityHadoopManager;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.IdentityMapHadoopJob;

@SuppressWarnings("rawtypes")
public class CanvasCodeManager implements GeneratedCodeManager {

  private final CanvasIdentityHadoopManager identity;

  public CanvasCodeManager() {
    this.identity = new CanvasIdentityHadoopManager();
  }

  @Override
  public Class<? extends IdentityMapHadoopJob> getIdentityMapHadoopJob() {
    return CanvasIdentityMapHadoopJob.class;
  }

  @Override
  public List<Class<? extends Mapper>> getIdentityMapperClasses() {
    return identity.getMapperClasses();
  }

  @Override
  public List<Class<? extends Mapper>> getIdentityScrubberClasses() {
    return identity.getScrubberClasses();
  }

  @Override
  public List<String> getIdentityTableNames() {
    return identity.getIdentityTableNames();
  }
}
