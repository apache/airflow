#!/usr/bin/env bash
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

# We are mounting /var/lib/docker and /tmp as tmpfs in order
# to gain speed when building the images The docker storage
# is ephemeral anyway and will be removed when instance stops

sudo service docker stop || true

sudo mount -t tmpfs -o size=3% tmpfs /tmp
sudo mount -t tmpfs -o size=85% tmpfs /var/lib/docker

sudo service docker start

# This instance will run for maximum 40 minutes and
# It will terminate itself after that (it can also
# be terminated immediately when the job finishes)
echo "sudo shutdown -h now" | at now +40 min
