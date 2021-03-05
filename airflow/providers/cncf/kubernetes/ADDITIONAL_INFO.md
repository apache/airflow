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

## Release 2021.3.3

### Bug fixes

* Allow pod name override in KubernetesPodOperator if pod_template is used. (#14186)
* Allow users of the KPO to *actually* template environment variables (#14083)

## Release 2021.2.5

  * `Pass image_pull_policy in KubernetesPodOperator correctly (#13289)`


### Bug fixes

## Additional limitations

This provider is only usable with Apache Airflow >= 1.10.12 version due to refactorings implemented in
Apache Airflow 1.10.11 and fixes implemented in 1.10.11. The package has appropriate requirements
set so you should not be able to install it with Apache Airflow < 1.10.12.
