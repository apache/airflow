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

# Airflow Sensors

Airflow Sensors are a special kind of Airflow Operator. When they run, they check to see if a certain criteria is met before they complete and let their downstream tasks execute. They are primarily used to enable portions of your DAG to wait for some criteria to be fulfilled by an external system.

The Sensors contained within this directory are core Airflow Sensors. They are included by default in any Airflow implementation. For other available sensors that have been built by the community, please see the `providers` directory.
