-console.log('Loading function');

var AWS = require('aws-sdk');

exports.handler = function(event, context) {
    // Find the Phase0ASGName Name by doing the following:
    //
    // 1. Get the CloudFormation stack name by looking at the prefix of this Lambda
    //    function. The first part of the function name (before the "-") is the
    //    same as the stack name.
    //
    // 2. Get the CloudFormation definition and find the value of the Output named
    //    "Phase0ASGName". That's our ASG for the Phase 0 EC2.
    //    We will increase the desired capacity of that ASG in order to run the
    //    Phase 0 operations.

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
                if (output.OutputKey == 'Phase0ASGName') {
                    var phaseZeroASGName = output.OutputValue;
                    console.log('Phase0ASGName Name: ' + phaseZeroASGName);

                    var autoscaling = new AWS.AutoScaling();

                    var params = {
                        AutoScalingGroupName: phaseZeroASGName,
                        MaxSize: 1
                    };
                    autoscaling.updateAutoScalingGroup(params, function(err, data) {
                        if (err) console.log(err, err.stack); // an error occurred
                        else {
                            var params = {
                                AutoScalingGroupName: phaseZeroASGName,
                                DesiredCapacity: 1
                            };
                            autoscaling.setDesiredCapacity(params, function(err, data) {
                                if (err) console.log(err, err.stack); // an error occurred
                                else console.log(data);

								var cfn2 = new AWS.CloudFormation();
								cfn2.describeStacks({
									StackName: stackName
								}, function(err, data) {
									if (err) {
										console.log(responseData.Error + ':\\n', err);
										context.fail('Error', responseData.Error + ': ' + err);
									} else {
										data.Stacks[0].Outputs.forEach(function(output) {
											if (output.OutputKey == 'SuccessSNSARN') {
												var phaseZeroSuccessSNSARN = output.OutputValue;
												console.log('Phase0SuccessSNS ARN: ' + phaseZeroSuccessSNSARN);

												var sns = new AWS.SNS();
												var params = {
													Message: "Starting Phase 0", 
													Subject: stackName + " - Starting Phase 0",
													TopicArn: phaseZeroSuccessSNSARN
												};
												sns.publish(params, function(err, data) {
															context.succeed();
												});
											}
										});
									}
								});
                            });
                        }
                    });
                }
            });
        }
    });
};