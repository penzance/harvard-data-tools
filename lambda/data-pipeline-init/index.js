-console.log('Loading function');

var AWS = require('aws-sdk');

function findAndReplaceStringValue(object, value, replacevalue) {
    for (var x in object) {
        if (typeof object[x] == typeof {}) {
            findAndReplaceStringValue(object[x], value, replacevalue);
        }
        if (typeof object[x] == "string") {
            if (object[x].indexOf(value) > -1 && x == "stringValue") {
                object["stringValue"] = object["stringValue"].replace(value, replacevalue);
                break; // uncomment to stop after first replacement
            }
        }
    }
}

exports.handler = function(event, context) {
    console.log('Object key: ', event.AWS_KEY);

    // Find the template Pipeline Id by doing the following:
    //
    // 1. Get the CloudFormation stack name by looking at the prefix of this Lambda
    //    function. The first part of the function name (before the "-") is the
    //    same as the stack name.
    //
    // 2. Get the CloudFormation definition and find the value of the Output named
    //    "DataPipelineId". That's our template pipeline id -- only one per stack --
    //    which we will use as a model for future pipelines.

    var stackName = context.functionName.substr(0, context.functionName.indexOf("-"));
    console.log("stackName: " + stackName);

    var cfn = new AWS.CloudFormation();
    cfn.describeStacks({
        StackName: stackName
    }, function(err, data) {
        if (err) {
            console.log(responseData.Error + ':\\n', err);
            context.fail('Error', responseData.Error + ': ' + err);
        } else {
            data.Stacks[0].Outputs.forEach(function(output) {
                if (output.OutputKey == 'DataPipelineId') {
                    var templatePipelineId = output.OutputValue;
                    console.log('Template Pipeline Id: ' + templatePipelineId);

                    // Get the template pipeline.

                    var datapipeline = new AWS.DataPipeline();

                    var paramsall = {
                        marker: ''
                    };

                    var params = {
                        pipelineId: templatePipelineId
                    };
                    datapipeline.getPipelineDefinition(params, function(err, definition) {
                        if (err) {
                            console.log(err, err.stack);
                            context.fail('Error', "Error getting pipeline definition: " + err);
                        } else {

                            // Setup the replacement values of tokens in the template
                            // pipeline definition.

                            var timeStampAsDate = new Date();
                            var fullObjectKey = event.AWS_KEY;

                            var directoryOfKey = "";
                            var fileKey;
                            if (fullObjectKey.indexOf("/") > -1) {
                                directoryOfKey = fullObjectKey.substr(0, fullObjectKey.lastIndexOf("/"));
                                fileKey = parseInt(fullObjectKey.substr(fullObjectKey.lastIndexOf("/") + 1), 10).toString();
                            }

                            var params = {
                                name: fileKey,
                                uniqueId: fileKey
                            };

                            // Create new pipeline and replace values of tokens in the
                            // template pipeline definition with actual values for this
                            // instantiation.

                            datapipeline.createPipeline(params, function(err, pipelineIdObject) {
                                if (err) {
                                    console.log(err, err.stack);
                                    context.fail('Error', "Error creating pipeline: " + err);
                                } else {
                                    console.log('New pipeline id: ' + pipelineIdObject.pipelineId);

                                    findAndReplaceStringValue(definition, "dataset-path-placeholder", fullObjectKey);
                                    // Run this again because dataset-path-placeholder is in our  MoveDatasetToArchives string twice!
                                    findAndReplaceStringValue(definition, "dataset-path-placeholder", fullObjectKey);
                                    findAndReplaceStringValue(definition, "dataset-id-placeholder", fileKey);

                                    // Place the modified pipeline definition into the
                                    // newly created pipeline and activate it.

                                    var params = {
                                            pipelineId: pipelineIdObject.pipelineId,
                                            pipelineObjects: definition.pipelineObjects, // (you can add parameter objects and values too)
                                            // parameterObjects: definition.parameterObjects, // need to remove these if they are empty, otherwise putPipelineDefinition fails
                                            // parameterValues: definition.parameterValues // need to remove these if they are empty, otherwise putPipelineDefinition fails
                                        } // Use definition from the getPipelineDefinition API result
                                    datapipeline.putPipelineDefinition(params, function(err, data) {
                                        if (err) {
                                            console.log(err, err.stack);
                                            context.fail('Error', "Error putting pipeline definition: " + err);
                                        } else {
                                            var emrNames = pipelineIdObject.pipelineId + '-EMR';
                                            var params = {
                                                pipelineId: pipelineIdObject.pipelineId,
                                                tags: [{
                                                    key: 'datapipeline-id',
                                                    value: pipelineIdObject.pipelineId
                                                    },
                                                    {
                                                    key: 'Name',
                                                    value: emrNames
                                                }, ]
                                            };
                                            datapipeline.addTags(params, function(err, data) {
                                                if (err) console.log(err, err.stack); // an error occurred
                                                else {
                                                    datapipeline.activatePipeline(pipelineIdObject, function(err, data) { // activate the pipeline finally
                                                        if (err) {
                                                            console.log(err, err.stack);
                                                            context.fail('Error', "Error activating pipeline: " + err);
                                                        } else console.log(data);
                                                        context.succeed();
                                                    });

                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        }
    });
};