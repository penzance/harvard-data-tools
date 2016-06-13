# See bootstrap lambda function for the exports to set.

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
java -Duser.timezone=$SERVER_TIMEZONE -Xmx$PHASE_0_HEAP_SIZE -cp /home/ec2-user/code/data_tools.jar $PHASE_0_CLASS $CONFIG_PATHS $DATA_SET_ID $PHASE_0_THREADS

# Spin up the pipeline
java -cp /home/ec2-user/code/data_tools.jar:$HARVARD_DATA_TOOLS_BASE/schema $PIPELINE_SETUP_CLASS $CONFIG_PATHS $HARVARD_DATA_TOOLS_BASE $RUN_ID $DATA_SET_ID

# Shut down; the pipeline will take over.
sudo halt now