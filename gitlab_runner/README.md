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

# Instructions on setting up GitLab runner on Kubernetes (GKE)

* Configure gcloud tool (https://cloud.google.com/sdk/install)
* Configure your project_id, cluster name and other parameters in [_config.sh](_config.sh)
* Create cluster in GKE by running [create_cluster.sh](create_cluster.sh)
* Run [install_rbac.sh](install_rbac.sh) to install tiller service account
* Copy [runner-registration-token-template.yaml](runner-registration-token-template.yaml) to
  [runner-registration-token.yaml](runner-registration-token.yaml)
* Add token available at your Settings -> CI/CD -> Runners page in 
  [runner-registration-token.yaml](runner-registration-token.yaml)
* Disable Shared Runners in Settings -> CI/CD -> Runners page
* Install runner on your : [./install_runner.sh](./install_runner.sh)
* Check that the runner is registered in Settings -> CI/CD -> Runners page

That's it. You should have GitLab Runner up and running.

Later when you need to update the runned (because of changes to [values.yaml](value.yaml)) use
[./upgrade_runner.sh](upgrade_runner.sh)
