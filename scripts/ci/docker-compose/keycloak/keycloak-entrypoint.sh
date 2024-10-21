#!/bin/bash

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

# We exit in case cd fails
cd /opt/keycloak/bin/ || exit

# Start Keycloak in the background
./kc.sh start-dev --http-port=38080 &

# Wait for Keycloak to be ready
echo "Waiting for Keycloak to start..."
while ! (echo > /dev/tcp/localhost/38080) 2>/dev/null; do
  echo "keycloak still not started"
  sleep 5
done
sleep 3
echo "Keycloak is running (probably...)"

# The below commands are used to disable the ssl requirement to use the admin panel of keycloak
echo "Configuring admin console access without ssl/https"
# Get credentials to make the below update to the realm settings
./kcadm.sh config credentials --server http://localhost:38080 --realm master --user admin --password admin
./kcadm.sh update realms/master -s sslRequired=NONE --server http://localhost:38080
echo "Configuring complete!"

# Keep the container running
wait
