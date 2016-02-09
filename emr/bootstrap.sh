#!/bin/bash

# add github.com to known_hosts and run ansible-pull
ssh-keyscan github.com >> /home/hadoop/.ssh/known_hosts

# install jdk and git
sudo yum install -y java-devel git-core

# clone our repo
git clone -b master https://github.com/penzance/harvard-data-tools.git /home/hadoop/harvard-data-tools
