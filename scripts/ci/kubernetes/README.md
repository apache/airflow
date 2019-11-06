<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
# Airflow on Kubernetes

If you don't have minikube installed, please run `./minikube/start_minikube.sh`
to start a k8s-instance on your local machine. Make sure that your `kubectl` is
pointing to the local k8s instance.

First build the docker images by running `./docker/build.sh`. This will build
the image and push it to the local registry.
Secondly, deploy Apache Airflow using `./kube/deploy.sh -d persistent_mode` or
`./kube/deploy.sh -d git_mode`.
Finally, open the Airflow webserver
page by browsing to `http://192.168.99.100:30809/admin/` (on OSX).

When kicking of a new job, you should be able to see new pods being kicked off:

```
$ kubectl get pods
NAME                                                                  READY     STATUS              RESTARTS   AGE
airflow-6cf894578b-rkcpm                                              2/2       Running             0          36m
examplehttpoperatorhttpsensorcheck-490dc90941984812b934fceedf07ca81   1/1       Running             0          7s
examplehttpoperatorhttpsensorcheck-ea787dd2163243a78dfba96d81f47e0d   1/1       Running             0          9s
examplehttpoperatorpostop-3637d44e1b8a42789c59d2c6a66bec6a            0/1       ContainerCreating   0          0s
postgres-airflow-b4844754f-b8d8k                                      1/1       Running             0          36m
```
