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

# Guard against a kubernetes cluster already being up
#!/usr/bin/env bash
kubectl get pods &> /dev/null
if [ $? -eq 0 ]; then
  echo "kubectl get pods returned 0 exit code, exiting early"
  exit 0
fi
#

curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && chmod +x minikube
curl -Lo kubectl  https://storage.googleapis.com/kubernetes-release/release/v1.7.0/bin/linux/amd64/kubectl && chmod +x kubectl

sudo mkdir -p /usr/local/bin
sudo mv minikube /usr/local/bin/minikube
sudo mv kubectl /usr/local/bin/kubectl

export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export MINIKUBE_HOME=$HOME
export CHANGE_MINIKUBE_NONE_USER=true
mkdir $HOME/.kube || true
touch $HOME/.kube/config

export KUBECONFIG=$HOME/.kube/config
sudo -E minikube start --vm-driver=none

# this for loop waits until kubectl can access the api server that minikube has created
for i in {1..150} # timeout for 5 minutes
do
  echo "------- Running kubectl get pods -------"
  kubectl get po &> /dev/null
  if [ $? -ne 1 ]; then
    break
  fi
  sleep 2
done
