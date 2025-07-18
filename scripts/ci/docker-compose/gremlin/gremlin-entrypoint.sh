#!/bin/sh

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
#!/bin/sh
set -eu

# Fix permissions on the config directory
echo "Fixing permissions for /opt/gremlin-server/conf..."
chmod -R a+rw /opt/gremlin-server/conf
ls -la /opt/gremlin-server/conf

# Start Gremlin Server in the background
echo "Starting Gremlin Server on port 8182..."
cd /opt/gremlin-server || exit
./bin/gremlin-server.sh conf/gremlin-server.yaml &

# Wait for Gremlin to be ready
echo "Waiting for Gremlin Server to start on port 8182..."
while ! nc -z gremlin 8182 2>/dev/null; do
  echo "Gremlin still not started"
  sleep 5
done
sleep 3
echo "Gremlin Server is running"

# Keep the container running
wait
