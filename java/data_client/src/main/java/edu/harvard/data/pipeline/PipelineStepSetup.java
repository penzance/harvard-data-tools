package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary;

public class PipelineStepSetup {

  public static void main(final String[] args)
      throws JsonParseException, JsonMappingException, IOException {
    final String pipelineId = args[0];
    final String dynamoTable = args[1];
    final String step = args[2];
    // TODO: Check args.

    // TODO: Check for table existence
    PipelineExecutionRecord.init(dynamoTable);

    final PipelineExecutionRecord record = PipelineExecutionRecord.find(pipelineId);
    // TODO: Check for missing record

    record.setPreviousSteps(getPreviousSteps(record));
    record.setCurrentStep(step);
    record.setCurrentStepStart(new Date());
    record.save();
  }

  private static String getPreviousSteps(final PipelineExecutionRecord record)
      throws JsonParseException, JsonMappingException, IOException {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setDateFormat(FormatLibrary.JSON_DATE_FORMAT);
    List<PreviousStepDescription> previous;
    if (record.getPreviousSteps() == null) {
      previous = new ArrayList<PreviousStepDescription>();
    } else {
      final TypeReference<List<PreviousStepDescription>> identiferTypeRef = new TypeReference<List<PreviousStepDescription>>() {
      };
      previous = jsonMapper.readValue(record.getPreviousSteps(), identiferTypeRef);
    }
    final String current = record.getCurrentStep();
    if (current != null) {
      final Date start = record.getCurrentStepStart();
      previous.add(new PreviousStepDescription(current, start, new Date()));
    }
    return jsonMapper.writeValueAsString(previous);
  }
}