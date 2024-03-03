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

Those are images that are needed for the Helm Chart.

In each of the images you can find "build_and_push.sh" script that builds and pushes the image.

You need to be a PMC with direct push access to "apache/airflow" DockerHub registry
to be able to push to the Airflow DockerHub registry.

You can set the DOCKERHUB_USER variable to push to your own DockerHub user if you want
 to test the image or build your own image.
