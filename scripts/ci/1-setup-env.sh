#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -exuo pipefail

# Start MiniCluster
java -cp "/tmp/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster > /dev/null &

# Set up ssh keys
echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ln -s ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
chmod 600 ~/.ssh/*

# SSH Service
sudo service ssh restart
