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

# Guard against a kubernetes cluster already being up
kubectl get pods &> /dev/null
if [ $? -eq 0 ]; then
  echo "kubectl get pods returned 0 exit code, exiting early"
  exit 0
fi
#

curl -Lo minikube https://storage.googleapis.com/minikube/releases/v0.24.1/minikube-linux-amd64 && chmod +x minikube
curl -Lo kubectl  https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/amd64/kubectl && chmod +x kubectl

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

start_minikube(){
  sudo -E minikube start --vm-driver=none --kubernetes-version="${KUBERNETES_VERSION}"

  # this for loop waits until kubectl can access the api server that minikube has created
  for i in {1..90} # timeout 3 minutes
  do
    echo "------- Running kubectl get pods -------"
    STDERR=$(kubectl get pods  2>&1 >/dev/null)
    if [ $? -eq 0 ]; then
      echo $STDERR

      # We do not need dynamic hostpath provisioning, so disable the default storageclass
      sudo -E minikube addons disable default-storageclass && kubectl delete storageclasses --all

      # We need to give permission to watch pods to the airflow scheduler.
      # The easiest way to do that is by giving admin access to the default serviceaccount (NOT SAFE!)
      kubectl create clusterrolebinding add-on-cluster-admin   --clusterrole=cluster-admin   --serviceaccount=default:default
      exit 0
    fi
    echo $STDERR
    sleep 2
  done
}

cleanup_minikube(){
  sudo -E minikube stop
  sudo -E minikube delete
  docker stop $(docker ps -a -q) || true
  docker rm $(docker ps -a -q) || true
  sleep 1
}

start_minikube
echo "Minikube cluster creation timedout. Attempting to restart the minikube cluster."
cleanup_minikube
start_minikube
echo "Minikube cluster creation timedout a second time. Failing."

exit 1
