package edu.harvard.data.canvas.cli;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

import org.kohsuke.args4j.Argument;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.io.FileTableWriter;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Events;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0MatterhornTableFactory;
import edu.harvard.data.matterhorn.bindings.phase0.Phase0Videos;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class ParseCommand implements Command {

  @Argument(index = 0, usage = "Matterhorn JSON file location on S3.", metaVar = "bucket/path/to/key", required = true)
  public String fileName;

  private final Set<String> seenVideos;

  public ParseCommand() {
    this.seenVideos = new HashSet<String>();
  }

  @Override
  public ReturnStatus execute(final DataConfiguration config, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException,
      VerificationException, ArgumentError {
    //    final AwsUtils aws = new AwsUtils();
    final File tmpFile = new File("/tmp/matterhorn");
    final File tmpEvents = new File("/tmp/events");
    final File tmpVideos = new File("/tmp/videos");
    // aws.getFile(AwsUtils.key(fileName.substring(0, fileName.indexOf("/")),
    // fileName.substring(fileName.indexOf("/") + 1)), tmpFile);
    final Phase0MatterhornTableFactory factory = new Phase0MatterhornTableFactory();
    final TableFormat format = new FormatLibrary().getFormat(Format.DecompressedCanvasDataFlatFiles);
    try (
        final BufferedReader in = new BufferedReader(
            new InputStreamReader((getInputStream(tmpFile))));
        TableWriter<Phase0Events> events = new FileTableWriter<Phase0Events>(Phase0Events.class,
            format, "events", tmpEvents);
        TableWriter<Phase0Videos> videos = new FileTableWriter<Phase0Videos>(Phase0Videos.class,
            format, "videos", tmpVideos);) {
      String line = in.readLine();
      while (line != null) {
        processLine(line, events, videos);
        line = in.readLine();
      }
    }
    return ReturnStatus.OK;
  }

  private void processLine(final String line, final TableWriter<Phase0Events> events,
      final TableWriter<Phase0Videos> videos)
          throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
    };
    try (ByteArrayInputStream in = new ByteArrayInputStream(line.getBytes())) {
      final Map<String, Object> value = jsonMapper.readValue(in, typeRef);
      final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'+00:00'");
      try {
        final Date parsedDate = dateFormat.parse((String) value.get("timestamp"));
        final Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
        final String videoId = (String) value.get("mpid");
        events.add(new Phase0Events(Long.valueOf((Integer) value.get("action_id")),
            videoId, timestamp, (String) value.get("ip"),
            (String) value.get("huid"), (String) value.get("session_id"),
            ((Integer) value.get("is_live")) != 0,
            (Integer) ((Map<String, Object>) value.get("action")).get("inpoint"),
            (Integer) ((Map<String, Object>) value.get("action")).get("outpoint"),
            (String) ((Map<String, Object>) value.get("action")).get("type"),
            (Integer) ((Map<String, Object>) value.get("action")).get("length"),
            (Boolean) ((Map<String, Object>) value.get("action")).get("is_playing"),
            (String) value.get("useragent")));

        if (value.containsKey("episode") && !((Map<String, Object>)value.get("episode")).isEmpty() && !seenVideos.contains(videoId)) {
          System.out.println(value);
          System.out.println(new Phase0Videos(
              videoId,
              (String) ((Map<String, Object>) value.get("episode")).get("series"),
              (String) ((Map<String, Object>) value.get("episode")).get("course"),
              (String) ((Map<String, Object>) value.get("episode")).get("type"),
              (String) ((Map<String, Object>) value.get("episode")).get("title"),
              (Integer) ((Map<String, Object>) value.get("episode")).get("year"),
              (String) ((Map<String, Object>) value.get("episode")).get("term"),
              (String) ((Map<String, Object>) value.get("episode")).get("cdn")).getFieldsAsList(new FormatLibrary().getFormat(Format.DecompressedCanvasDataFlatFiles)));

          seenVideos.add(videoId);
        }
      } catch (final ParseException e) {
        e.printStackTrace();
      }

    }
  }

  private InputStream getInputStream(final File tmpFile) throws FileNotFoundException, IOException {
    if (fileName.endsWith("gz")) {
      return new GZIPInputStream(new FileInputStream(tmpFile));
    } else {
      return new FileInputStream(tmpFile);
    }
  }

  @Override
  public String getDescription() {
    return "Parse a single file's worth of Matterhorn events";
  }

}
