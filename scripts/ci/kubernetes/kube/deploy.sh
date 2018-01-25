#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

IMAGE=${1:-airflow/ci}
TAG=${2:-latest}
DIRNAME=$(cd "$(dirname "$0")"; pwd)

# create an emptydir for postgres to store it's volume data in
sudo mkdir -p /data/postgres-airflow

mkdir -p $DIRNAME/.generated
kubectl apply -f $DIRNAME/postgres.yaml
sed -e "s#{{docker_image}}#$IMAGE#g" -e "s#{{docker_tag}}#$TAG#g" $DIRNAME/airflow.yaml.template > $DIRNAME/.generated/airflow.yaml && kubectl apply -f $DIRNAME/.generated/airflow.yaml

# wait for up to 10 minutes for everything to be deployed
for i in {1..150}
do
  echo "------- Running kubectl get pods -------"
  PODS=$(kubectl get pods | awk 'NR>1 {print $0}')
  echo "$PODS"
  NUM_AIRFLOW_READY=$(echo $PODS | grep airflow | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  NUM_POSTGRES_READY=$(echo $PODS | grep postgres | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  if [ "$NUM_AIRFLOW_READY" == "1" ] && [ "$NUM_POSTGRES_READY" == "1" ]; then
    break
  fi
  sleep 4
done
