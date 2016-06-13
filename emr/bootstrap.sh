#!/bin/bash

export DATA_SCHEMA_VERSION=$1  # Used by code generator
export GIT_BRANCH=$2
export GENERATOR=$3
export CONFIG_PATHS=$4 # Used by code generator
export RUN_ID=$5 # Used by code generator
export HARVARD_DATA_GENERATED_OUTPUT=$6  # Used by code generator

# - HARVARD_DATA_TOOLS_BASE: the directory where the harvard-data-tools
#     repository has been checked out. Point to the root of the repository, e.g.
#     /tmp/code/harvard-data-tools
export HARVARD_DATA_TOOLS_BASE=/home/hadoop/harvard-data-tools

# - SECURE_PROPERTIES_LOCATION: a directory containing the secure.properties
#     file modelled after the template in:
#     java/aws_data_tools/src/main/resources/secure.properties.example
export SECURE_PROPERTIES_LOCATION=/home/hadoop

# - DATA_SCHEMA_VERSION: The version of the data schema for which
#     files will be generated. Format as a string, e.g. 1.2.0

# - HDFS_PHASE_n_DIR: HDFS directories for data in each phase n
export HDFS_PHASE_0_DIR=hdfs:///phase_0
export HDFS_PHASE_1_DIR=hdfs:///phase_1
export HDFS_PHASE_2_DIR=hdfs:///phase_2

# add github.com to known_hosts
ssh-keyscan github.com >> /home/hadoop/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

#Install maven via instructions at https://gist.github.com/sebsto/19b99f1fa1f32cae5d00
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven

# clone our repo
git clone -b $GIT_BRANCH https://github.com/penzance/harvard-data-tools.git $HARVARD_DATA_TOOLS_BASE

# generate the tools
python /home/hadoop/harvard-data-tools/python/$GENERATOR
chmod 764 $HARVARD_DATA_GENERATED_OUTPUT/*.sh

# setup CloudWatch logging
sudo yum install -y awslogs
sudo cp /home/hadoop/harvard-data-tools/cloudwatch/awslogs-emr.conf /etc/awslogs/awslogs.conf
sudo service awslogs start

# Create a SQLActivity script for the Data Pipeline that is customized for this data set
# NOTE: It would have been preferrable to have a generic script which accepts scriptArguments,
#       but after much troubleshooting, that appeared to be impossible, so this approach was created.

# Get DUMPID from tags
# DUMPID=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$(curl http://169.254.169.254/latest/meta-data/instance-id)" "Name=key,Values=dump-id" --query 'Tags[*].[Value]' --output text --region us-east-1)

# Get FULLDUMPID from tags
#FULLDUMPPATH=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$(curl http://169.254.169.254/latest/meta-data/instance-id)" "Name=key,Values=full-dump-path" --query 'Tags[*].[Value]' --output text --region us-east-1)

# Credit for the following grep magic goes to http://stackoverflow.com/a/20488535
# Get the path to the data files in the intermediate S3 bucket, assign to env variable
#DATASETPATH=$(grep -Po '(?<=\"AWS_KEY\" : \")[^\"]*' /root/the_result.json)
# Get the data set number, assign to env variable
#DATASETNUM=$(grep -Po '(?<=\"DUMP_SEQUENCE\" : \")[^\"]*' /root/the_result.json)

# Replace <intermediates3bucketandpath> placeholder with real intermediate bucket name and path
#sed -i "s@<intermediates3bucketandpath>/@s3://$INTERMEDIATE_BUCKET/$FULLDUMPPATH@g" /home/hadoop/s3_to_redshift_loader.sql

# Replace <awskeyandsecret> placeholder with real creds
#sed -i "s@<awskeyandsecret>@aws_access_key_id=$AWS_ACCESS_KEY;aws_secret_access_key=$AWS_SECRET_KEY@g" /home/hadoop/s3_to_redshift_loader.sql

# Copy modified file to code S3 bucket, name it with data set number.
#if [ "$DUMPID" != "" ]; then
#  aws s3 cp /home/hadoop/s3_to_redshift_loader.sql s3://$CODE_BUCKET/$DUMPID.sql
#else
#  aws s3 cp /home/hadoop/s3_to_redshift_loader.sql s3://$CODE_BUCKET/matterhorn.sql
#fi
