#!/bin/bash

# add github.com to known_hosts and run ansible-pull
ssh-keyscan github.com >> /root/.ssh/known_hosts

# install jdk and git
yum install -y java-devel git-core

# clone our repo
git clone -b master https://github.com/penzance/harvard-data-tools.git /root/harvard-data-tools
