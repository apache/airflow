#!/usr/bin/env bash

wget https://cdn.rawgit.com/kubernetes-sigs/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh

chmod +x dind-cluster-v1.10.sh
./dind-cluster-v1.8.sh up
export PATH="$HOME/.kubeadm-dind-cluster:$PATH"
