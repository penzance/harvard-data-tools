#! /bin/bash

export GIT_BRANCH=api_pipeline
export HARVARD_DATA_TOOLS_BASE=/home/ec2-user/harvard-data-tools
export GENERATOR=canvas_generate_tools.py
export PHASE_0=canvas_phase_0.py
export DATA_SCHEMA_VERSION=1.10.2
export CONFIG_PATHS="s3://hdt-code/api_pipeline/canvas.properties|s3://hdt-code/api_pipeline/secure.properties"
export HARVARD_DATA_GENERATED_OUTPUT=/home/ec2-user/code
export DATA_SET_ID=29781907-7b5b-4370-b7f2-4e28b5116396
export PHASE_0_THREADS=1
export PHASE_0_HEAP_SIZE=512m
export PIPELINE_ID="TestPipeline"

echo "Here"

# add github.com to known_hosts
ssh-keyscan github.com >> /home/ec2-user/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

#Install maven via instructions at https://gist.github.com/sebsto/19b99f1fa1f32cae5d00
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven

# clone our repo
git clone -b $GIT_BRANCH https://github.com/penzance/harvard-data-tools.git $HARVARD_DATA_TOOLS_BASE

# generate the tools
python $HARVARD_DATA_TOOLS_BASE/python/$GENERATOR

# run phase 0
java -Duser.timezone=$SERVER_TIMEZONE -Xmx$PHASE_0_HEAP_SIZE -cp /home/ec2-user/code/data_tools.jar edu.harvard.data.canvas.CanvasPhase0 $CONFIG_PATHS $DATA_SET_ID $PHASE_0_THREADS

# sudo halt now