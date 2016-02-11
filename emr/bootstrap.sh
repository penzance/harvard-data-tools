#!/bin/bash

# - HARVARD_DATA_TOOLS_BASE: the directory where the harvard-data-tools
#     repository has been checked out. Point to the root of the repository, e.g.
#     /tmp/code/harvard-data-tools
export HARVARD_DATA_TOOLS_BASE=/home/hadoop/harvard-data-tools

# - SECURE_PROPERTIES_LOCATION: a directory containing the secure.properties
#     file modelled after the template in:
#     java/aws_data_tools/src/main/resources/secure.properties.example
export SECURE_PROPERTIES_LOCATION=/home/hadoop

# - HARVARD_DATA_GENERATED_OUTPUT: the directory where generated scripts and
#     .jar files should be stored.
export HARVARD_DATA_GENERATED_OUTPUT=/home/hadoop

# - CANVAS_DATA_SCHEMA_VERSION: The version of the Canvas Data schema for which
#     files will be generated. Format as a string, e.g. 1.2.0
export CANVAS_DATA_SCHEMA_VERSION=<canvas_data_schema_version>

# add github.com to known_hosts and run ansible-pull
ssh-keyscan github.com >> /home/hadoop/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

# clone our repo
git clone -b master https://github.com/penzance/harvard-data-tools.git /home/hadoop/harvard-data-tools

#Install maven via instructions at https://gist.github.com/sebsto/19b99f1fa1f32cae5d00
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven

# copy DownloadVerifySecureProperties from S3 to the local FS
aws s3 cp s3://<code_s3_bucket>/secure.properties /home/hadoop/.

# generate the tools
python /home/hadoop/harvard-data-tools/python/generate_tools.py

#copy appropriate files to temp S3 bucket here
aws s3 cp /home/hadoop/create_tables.q s3://<new_code_s3_bucket>/create_tables.q

aws s3 cp /home/hadoop/canvas_data_hadoop.jar s3://<new_code_s3_bucket>/canvas_data_hadoop.jar
aws s3 cp /home/hadoop/canvas_phase_1_hive.q s3://<new_code_s3_bucket>/canvas_phase_1_hive.q

aws s3 cp /home/hadoop/canvas_phase_2_hive.q s3://<new_code_s3_bucket>/canvas_phase_2_hive.q
