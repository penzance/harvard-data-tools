#!/bin/bash

# This script is called by the bootstrapAction attribute on the EMR cluster in a data pipeline.
# See java/data_client/src/main/java/edu/harvard/data/pipeline/Pipeline.java

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

# - HDFS_PHASE_n_DIR: HDFS directories for data in each phase n
export HDFS_PHASE_0_DIR=hdfs:///phase_0
export HDFS_PHASE_1_DIR=hdfs:///phase_1
export HDFS_PHASE_2_DIR=hdfs:///phase_2

# Dump the environment for the sake of the logs
env

# add github.com to known_hosts
ssh-keyscan github.com >> /home/hadoop/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

# clone our repo
git clone -b $GIT_BRANCH https://github.com/penzance/harvard-data-tools.git $HARVARD_DATA_TOOLS_BASE

# setup CloudWatch logging
sudo yum install -y awslogs
sudo cp /home/hadoop/harvard-data-tools/cloudwatch/awslogs-emr.conf /etc/awslogs/awslogs.conf
sudo service awslogs start

#Install maven via instructions at https://gist.github.com/sebsto/19b99f1fa1f32cae5d00
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven

# generate the tools
python /home/hadoop/harvard-data-tools/python/$GENERATOR &> /var/log/generate-tools.out
chmod 764 $HARVARD_DATA_GENERATED_OUTPUT/*.sh
