#!/usr/bin/env bash
_MY_SCRIPT="${BASH_SOURCE[0]}"
_MY_DIR=$(cd "$(dirname "$_MY_SCRIPT")" && pwd)
_UNAME_OUT=$(uname -s)
case "${_UNAME_OUT}" in
    Linux*)     _MY_OS=linux;;
    Darwin*)    _MY_OS=darwin;;
    *)          echo "${_UNAME_OUT} is not unsupported."
                exit 1;;
esac
echo "Local OS is ${_MY_OS}"
_KUBERNETES_VERSION="${KUBERNETES_VERSION}"
if [ "$_MY_OS" = "linux" ]; then
    export _REGISTRY_IP="10.192.0.1"
else
    export _REGISTRY_IP=`ipconfig getifaddr en0`
fi

cd $_MY_DIR
rm -rf tmp
mkdir -p bin tmp

rm $DIRNAME/dind-cluster-*
wget https://cdn.rawgit.com/kubernetes-sigs/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh

chmod +x $_MY_DIR/dind-cluster-v1.10.sh
$_MY_DIR/dind-cluster-v1.10.sh clean
echo "registries are "[\"${_REGISTRY_IP}:5000\"]""
DIND_INSECURE_REGISTRIES="[\"${_REGISTRY_IP}:5000\"]" DIND_SUBNET="10.192.0.0" DIND_SUBNET_SIZE=16 $_MY_DIR/dind-cluster-v1.10.sh up
export PATH="$HOME/.kubeadm-dind-cluster:$PATH"

if [[ ! -x /usr/local/bin/kubectl ]]; then
  echo Downloading kubectl, which is a requirement for using minikube.
  curl -Lo bin/kubectl  \
    https://storage.googleapis.com/kubernetes-release/release/${_KUBERNETES_VERSION}/bin/${_MY_OS}/amd64/kubectl
  chmod +x bin/kubectl
  sudo mv bin/kubectl /usr/local/bin/kubectl
fi

REG=`docker ps -f name=registry -q`

if [ -n "$REG" ]; then
    docker stop $REG; docker rm $REG
fi

if [ "$_MY_OS" = "linux" ]; then
    docker run -d -p :5000:5000 --restart=always --name registry registry:2
else
    docker run -d -p 5000:5000 --restart=always --name registry registry:2
fi
