#!/usr/bin/env bash

_UNAME_OUT=$(uname -s)
case "${_UNAME_OUT}" in
    Linux*)     _MY_OS=linux;;
    Darwin*)    _MY_OS=darwin;;
    *)          echo "${_UNAME_OUT} is unsupported."
                exit 1;;
esac

if [ "$_MY_OS" = "linux" ]; then
    export _REGISTRY_IP=10.192.0.1
else
    export _REGISTRY_IP=`ipconfig getifaddr en0`
fi
echo "Deploying insecure registry"
if [ "$_MY_OS" = "linux" ]; then
    DIRNAME=$(cd "$(dirname "$0")"; pwd)
#    sudo rm /etc/docker/daemon.json
#    sudo cp $DIRNAME/daemon.json /etc/docker/
    sudo sed -i "s/\DOCKER_OPTS=\"/DOCKER_OPTS=\"--insecure-registry=$_REGISTRY_IP:5000 /g" /etc/default/docker
    sudo cat /etc/default/docker
    sudo service docker restart
fi



