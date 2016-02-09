# canvas-data

## Assumptions

### Your Amazon Web Services environment
This code assumes you have an AWS VPC with at least two public subnets, two private subnets and two database subnets across two availability zones.

### Your AWS account
I don't have exact details at the moment, but your account needs to be fairly powerful, able to create a CloudFormation stack and it's resources, including IAM resources.

### Your local environment
This code assumes you have the AWS Command Line Interface tools (https://aws.amazon.com/cli/) installed locally, the CLI is already configured to use your credentials.

### AWS cost and IAM warnings
Please note this CloudFormation spins up a number of resources, including IAM resources that could have access to unrelated resources in the same AWS account. It is your responsibility (and a good idea) to review the costs associated with the resources defined in this stack and the permissions given by the IAM roles in this stack.

## How to stand up the infrastructure in your AWS VPC

1. Make an S3 bucket to store code and note the location (e.g., `s3://my-awesome-bucket`)
2. Place the following files in the root of the code S3 bucket unless otherwise noted:
  * `cloudformation/environment.json`
  * `cloudwatch/awslogs.conf`
  * `lambda/download-verify/download-verify-lambda.zip` (NOTE: This file is the result of running `zip -r download-verify-lambda.zip .` inside the `lambda/download-verify` directory.)
  * `lambda/data-pipeline-init/data-pipeline-init-lambda.zip` (NOTE: This file is the result of running `zip -r data-pipeline-init-lambda-code.zip .` inside the `lambda/data-pipeline-init` directory.)
  * `python/download_and_verify.py`
  * `aws_data_tools-A.B.C.jar`
  * `canvas_data_schema_bindings-X.Y.Z.jar`
  * `secure.properties` based on `java/aws_data_tools/src/main/resources/secure.properties.example`
  * Hadoop JAR files - details TBD
  * Hive/Pig scripts - details TBD
3. Create a customized CloudFormation parameters file by doing the following:
  * Create a `cloudformation/myparameters` directory locally (it and its future contents will be .gitignore'd)
  * Copy `cloudformation/parameters.json.example` to `cloudformation/myparameters/my-custom-parameters.json` edit it to include your specific parameter values
4. Create the CloudFormation stack by running:
```aws cloudformation create-stack --stack-name mystack --template-url https://my-awesome-bucket.s3.amazonaws.com/environment.json --parameters file:///path/to/cloudformation/myparameters/my-custom-parameters.json --region us-east-1 --capabilities CAPABILITY_IAM```
5. Once the stack is up, cron functionality to process data sets on a schedule must be setup manually. To accomplish this:
  * Login to the AWS CloudWatch console, go to Events, then Rules
  * Click Create a Rule
  * Select Schedule for event source, create the desired schedule (e.g., a cron to run daily at 0700UTC - `00 07 * * ? *`)
  * Add a Target and select the DownloadVerifyLambda function created by the CloudFormation stack
  * Click Configure details and then name and describe the rule, and ensure the Enabled checkbox is checked
  * Click Create rule to save it

Once stood up, the entire process can be manually triggered by initiating a Test of the DownloadVerifyLambda function. It does not process input JSON, so Test Event data can be any syntactically-valid JSON.
