# See bootstrap lambda function for the exports to set.

# Dump the environment for the sake of the logs
env

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

# Install the Oracle JDBC drivers
aws s3 cp s3://hdt-code/ojdbc7.jar /home/ec2-user/ojdbc7.jar
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.1 -Dpackaging=jar -Dfile=/home/ec2-user/ojdbc7.jar -DgeneratePom=true

# clone our repo
git clone -b $GIT_BRANCH https://github.com/penzance/harvard-data-tools.git $HARVARD_DATA_TOOLS_BASE

# setup CloudWatch logging
sudo yum install -y awslogs
sudo cp /home/ec2-user/harvard-data-tools/cloudwatch/awslogs-phase0.conf /etc/awslogs/awslogs.conf
sudo service awslogs start

# generate the tools
python $HARVARD_DATA_TOOLS_BASE/python/$GENERATOR &> /var/log/generate-tools.out

# run phase 0
java -Duser.timezone=$SERVER_TIMEZONE -Xmx$PHASE_0_HEAP_SIZE -cp /home/ec2-user/code/data_tools.jar $PHASE_0_CLASS $CONFIG_PATHS $RUN_ID $DATA_SET_ID $PHASE_0_THREADS $CODE_MANAGER_CLASS &> /var/log/phase0-output.log

if [ $CREATE_PIPELINE -eq 1 ]
then
    # Spin up the pipeline
    java -cp /home/ec2-user/code/data_tools.jar:$HARVARD_DATA_TOOLS_BASE/schema edu.harvard.data.pipeline.DataPipelineSetup $CONFIG_PATHS $RUN_ID $CODE_MANAGER_CLASS &> /var/log/pipeline-init.out
fi

# Pause before shutting down to make sure that logs are all flushed
sleep 60

# Shut down; the pipeline will take over.
sudo halt now