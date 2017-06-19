#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a script for testing Elasticsearch logging backend using Docker.
# Check if user has docker installed
docker -v 1>/dev/null
if [ $? -ne 0 ]; then
  echo "Please install Docker to run the unit test. Abort"
  exit
fi
# Get elasticsearch running in docker
echo "Starting Elasticsearch Docker image..."
CONTAINER_ID=$(docker run -d \
               -p 9200:9200 \
               -e "cluster.name=airflow" \
               -e "http.host=0.0.0.0" \
               -e "transport.host=127.0.0.1" \
               -e "xpack.security.enabled=false" \
               -e "xpack.monitoring.enabled=false" \
               -e "xpack.ml.enabled=false" \
               -e "xpack.graph.enabled=false" \
               -e "xpack.watcher.enabled=false" \
               docker.elastic.co/elasticsearch/elasticsearch:5.4.1)
if [ $? -ne 0 ]; then
  echo "Abort."
  exit
fi
echo "Container ${CONTAINER_ID} started."
# Wait for Elasticsearch to start (~20s)
echo "Waiting for Elasticsearch to initialize..."
echo -ne '##                        (18%)\r'
sleep 3
echo -ne '#####                     (25%)\r'
sleep 3
echo -ne '########                  (42%)\r'
sleep 3
echo -ne '#############             (66%)\r'
sleep 3
echo -ne '################          (78%)\r'
sleep 3
echo -ne '###################       (89%)\r'
sleep 3
echo -ne '#####################     (95%)\r'
sleep 3
echo -ne '#######################   (100%)\r'
echo -ne '\n'
# Run integration test
echo "Running Elasticsearch logging backend test..."
export AIRFLOW_RUNALL_TESTS=1
python test_elasticsearch_logging_backend.py
export AIRFLOW_RUNALL_TESTS=0
# Stop container
docker stop $CONTAINER_ID 1>/dev/null
if [ $? -ne 0 ]; then
  echo "Failed to stop container ${CONTAINER_ID}. Please clean up manually."
else
  echo "Container ${CONTAINER_ID} stopped."
fi
