# See bootstrap lambda function for the exports to set.

# setup CloudWatch logging
sudo yum install -y awslogs
sudo cp /home/hadoop/harvard-data-tools/cloudwatch/awslogs-phase0.conf /etc/awslogs/awslogs.conf
sudo service awslogs start

# add github.com to known_hosts
ssh-keyscan github.com >> /home/ec2-user/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

#Install maven via instructions at https://gist.github.com/sebsto/19b99f1fa1f32cae5d00
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
# Grab a cached version of the .m2 directory if available in order to speed up the build
if [ -n "$MAVEN_REPO_CACHE" ]
then
    cd ~root
    aws s3 cp $MAVEN_REPO_CACHE maven_cache.tgz
    tar -xzf maven_cache.tgz
    cd -
fi

# clone our repo
git clone -b $GIT_BRANCH https://github.com/penzance/harvard-data-tools.git $HARVARD_DATA_TOOLS_BASE

# generate the tools
python $HARVARD_DATA_TOOLS_BASE/python/$GENERATOR

# run phase 0
java -Duser.timezone=$SERVER_TIMEZONE -Xmx$PHASE_0_HEAP_SIZE -cp /home/ec2-user/code/data_tools.jar $PHASE_0_CLASS $CONFIG_PATHS $RUN_ID $DATA_SET_ID $PHASE_0_THREADS

if [ $CREATE_PIPELINE -eq 1 ]
then
    # Spin up the pipeline
    java -cp /home/ec2-user/code/data_tools.jar:$HARVARD_DATA_TOOLS_BASE/schema $PIPELINE_SETUP_CLASS $CONFIG_PATHS $HARVARD_DATA_TOOLS_BASE $RUN_ID $DATA_SET_ID
fi

# Shut down; the pipeline will take over.
sudo halt now