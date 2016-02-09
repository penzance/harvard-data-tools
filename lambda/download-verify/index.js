-console.log('Loading function');

var AWS = require('aws-sdk');

exports.handler = function(event, context) {
    // Find the DownloadVerifyASG Name by doing the following:
    //
    // 1. Get the CloudFormation stack name by looking at the prefix of this Lambda
    //    function. The first part of the function name (before the "-") is the
    //    same as the stack name.
    //
    // 2. Get the CloudFormation definition and find the value of the Output named
    //    "DownloadVerifyASGName". That's our ASG for the DownloadVerify EC2.
    //    We will increase the desired capacity of that ASG in order to run the
    //    DownloadVerify operations.

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
                if (output.OutputKey == 'DownloadVerifyASGName') {
                    var downloadVerifyASGName = output.OutputValue;
                    console.log('DownloadVerifyASG Name: ' + downloadVerifyASGName);

                    var autoscaling = new AWS.AutoScaling();

                    var params = {
                        AutoScalingGroupName: downloadVerifyASGName,
                        MaxSize: 1
                    };
                    autoscaling.updateAutoScalingGroup(params, function(err, data) {
                        if (err) console.log(err, err.stack); // an error occurred
                        else {
                            var params = {
                                AutoScalingGroupName: downloadVerifyASGName,
                                DesiredCapacity: 1
                            };
                            autoscaling.setDesiredCapacity(params, function(err, data) {
                                if (err) console.log(err, err.stack); // an error occurred
                                else console.log(data);
                                context.succeed();
                            });
                        }
                    });
                }
            });
        }
    });
};