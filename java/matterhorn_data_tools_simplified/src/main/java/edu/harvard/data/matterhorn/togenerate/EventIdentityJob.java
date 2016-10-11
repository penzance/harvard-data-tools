package edu.harvard.data.matterhorn.togenerate;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.TableFormat;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.S3TableReader;
import edu.harvard.data.io.S3TableWriter;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.bindings.deidentified.DeidentifiedEvent;
import edu.harvard.data.matterhorn.bindings.input.InputEvent;

public class EventIdentityJob implements Callable<Void> {

  private final IdentityService idService;
  private final S3ObjectId inLocation;
  private final S3ObjectId outLocation;
  private final AwsUtils aws;
  private final DataConfig config;
  private final TableFormat format;

  public EventIdentityJob(final IdentityService idService, final S3ObjectId baseInLocation,
      final S3ObjectId baseOutLocation, final DataConfig config) {
    this.idService = idService;
    this.inLocation = AwsUtils.key(baseInLocation, "event");
    this.outLocation = AwsUtils.key(baseInLocation, "event");
    this.config = config;
    this.aws = new AwsUtils();
    this.format = new FormatLibrary().getFormat(config.getPipelineFormat());
  }

  @Override
  public Void call() throws Exception {
    for (final S3ObjectSummary idMapFile : aws.listKeys(inLocation)) {
      final String[] keyParts = idMapFile.getKey().split("/");
      final S3ObjectId inLocation = AwsUtils.key(idMapFile);
      final S3ObjectId outFile = AwsUtils.key(outLocation, keyParts[keyParts.length - 1]);
      final File tmpDir = new File(config.getScratchDir() + "/event");
      final File tmpFile = new File(tmpDir, UUID.randomUUID().toString());
      try (
          TableReader<InputEvent> in = new S3TableReader<InputEvent>(aws, InputEvent.class, format,
              inLocation, tmpDir);
          TableWriter<DeidentifiedEvent> out = new S3TableWriter<DeidentifiedEvent>(
              DeidentifiedEvent.class, format, outFile, tmpFile)) {
        for (final InputEvent record : in) {
          final IdentityMap id = new IdentityMap();
          id.set(IdentifierType.HUID, record.getHuid());
          final String researchUuid = idService.getResearchUuid(id, config.getMainIdentifier());

          final DeidentifiedEvent deidentified = new DeidentifiedEvent(record);
          deidentified.setHuidResearchUuid(researchUuid);
        }
      }
    }
    return null;
  }

}
