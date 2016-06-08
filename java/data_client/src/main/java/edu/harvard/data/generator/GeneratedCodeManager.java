package edu.harvard.data.generator;

import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.identity.IdentityMapHadoopJob;

@SuppressWarnings("rawtypes")
public interface GeneratedCodeManager {
  List<Class<? extends Mapper>> getIdentityMapperClasses();
  List<Class<? extends Mapper>> getIdentityScrubberClasses();
  List<String> getIdentityTableNames();
  Class<? extends IdentityMapHadoopJob> getIdentityMapHadoopJob();
}
