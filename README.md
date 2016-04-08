# harvard-data-tools

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
  * `emr/bootstrap.sh`
  * `lambda/download-verify/download-verify-lambda.zip` (NOTE: This file is the result of running `zip -r download-verify-lambda.zip .` inside the `lambda/download-verify` directory.)
  * `lambda/data-pipeline-init/data-pipeline-init-lambda.zip` (NOTE: This file is the result of running `zip -r data-pipeline-init-lambda-code.zip .` inside the `lambda/data-pipeline-init` directory.)
  * `secure.properties` based on `java/aws_data_tools/src/main/resources/secure.properties.example`
  * `extra_keys`, a custom file containing SSH keys which should be installed on the EMR cluster, most useful for debugging
3. Create a customized CloudFormation parameters file by doing the following:
  * Create a `cloudformation/myparameters` directory locally (it and its future contents will be .gitignore'd)
  * Copy `cloudformation/parameters.json.example` to `cloudformation/myparameters/my-custom-parameters.json` edit it to include your specific parameter values
4. Create the CloudFormation stack by running:
```aws cloudformation create-stack --stack-name mystack --template-url https://my-awesome-bucket.s3.amazonaws.com/environment.json --parameters file:///path/to/cloudformation/myparameters/my-custom-parameters.json --region us-east-1 --capabilities CAPABILITY_IAM```
5. Edit the `secure.properties` file created above with updated values for some resources which were created during the CloudFormation spin up.
6. Once the stack is up, cron functionality to process data sets on a schedule must be setup manually. To accomplish this:
  * Login to the AWS CloudWatch console, go to Events, then Rules
  * Click Create a Rule
  * Select Schedule for event source, create the desired schedule (e.g., a cron to run daily at 0700UTC - `00 07 * * ? *`)
  * Add a Target and select the DownloadVerifyLambda function created by the CloudFormation stack
  * Click Configure details and then name and describe the rule, and ensure the Enabled checkbox is checked
  * Click Create rule to save it

Once stood up, the entire process can be manually triggered by initiating a Test of the DownloadVerifyLambda function. It does not process input JSON, so Test Event data can be any syntactically-valid JSON.

## Miscellaneous

* Considerations when ingesting a large data set

When importing large data sets ( > 100GB ), it may be worth considering breaking up the process into multiple steps in order to minimize EMR cluster costs. For example, you may want to follow this procedure:
 * Perform the "download and verify" step, and start the Data Pipeline as usual.
 * Monitor the progress of the Data Pipeline and terminate it after the "Move Data from HDFS" step. At this point, data is ready to be loaded from the intermediate S3 bucket into Redshift.
 * Using your favorite Postgres tool connected to your Redshift cluster, run the Redshift load step manually by copying and pasting the SQL into the tool and executing. For larger datasets, this will take a long time. By running this manually, you are not incurring charges for the EMR cluster while Redshift reads from S3.
 * Once complete, manually complete the remaining pipeline steps, which are essentially cleanup steps that move and delete various data.

You may also consider increasing the type and number of nodes in your Redshift cluster before the load, and then returning the cluster to it's pre-load state after the load is successful. More resources in the Redshift cluster are likely to increase throughput and make the process faster.

* Consider using Redshift reserved instances to decrease cost

If you can estimate your average Redshift configuration -- e.g., after doing some baseline analysis, you determine 5 dc1.large nodes should adequately serve the requirements for the next year -- you should consider using reserved instances to minimize cost (probably at a 20-30% discount)

* Consider Redshift optimizations, see https://blogs.aws.amazon.com/bigdata/post/Tx31034QG0G3ED1/Top-10-Performance-Tuning-Techniques-for-Amazon-Redshift for suggestions
