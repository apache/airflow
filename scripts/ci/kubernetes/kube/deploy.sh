#!/usr/bin/env bash
#
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
_UNAME_OUT=$(uname -s)
case "${_UNAME_OUT}" in
    Linux*)     _MY_OS=linux;;
    Darwin*)    _MY_OS=darwin;;
    *)          echo "${_UNAME_OUT} is not unsupported."
                exit 1;;
esac
echo "Local OS is ${_MY_OS}"

if [ "$_MY_OS" = "linux" ]; then
    export _REGISTRY_IP=10.192.0.1
else
    export _REGISTRY_IP=`ipconfig getifaddr en0`
fi

IMAGE=${1:-airflow/ci}
TAG=${2:-latest}
DIRNAME=$(cd "$(dirname "$0")"; pwd)
rm $DIRNAME/airflow.yaml
sed -e s/{REG_IP}/$_REGISTRY_IP/g $DIRNAME/airflow.yaml.template > $DIRNAME/airflow.yaml
cat $DIRNAME/airflow.yaml

kubectl delete -f $DIRNAME/postgres.yaml
kubectl delete -f $DIRNAME/airflow.yaml
kubectl delete -f $DIRNAME/secrets.yaml

kubectl apply -f $DIRNAME/secrets.yaml
kubectl apply -f $DIRNAME/configmaps.yaml
kubectl apply -f $DIRNAME/postgres.yaml
kubectl apply -f $DIRNAME/volumes.yaml
kubectl apply -f $DIRNAME/airflow.yaml

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

POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow | head -1)

echo "------- pod description -------"
kubectl describe pod $POD
echo "------- webserver logs -------"
kubectl logs $POD webserver
echo "------- scheduler logs -------"
kubectl logs $POD scheduler
echo "--------------"
