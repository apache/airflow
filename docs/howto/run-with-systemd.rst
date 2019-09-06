 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Running Airflow with systemd
============================

Airflow can integrate with systemd based systems. This makes watching your
daemons easy as `systemd` can take care of restarting a daemon on failures.

In the ``scripts/systemd`` directory, you can find unit files that
have been tested on Redhat based systems. These files can be used as-is by copying them over to
``/usr/lib/systemd/system``.

The following **assumptions** have been made while creating these unit files:

#. Airflow runs as the following `user:group` ``airflow:airflow``.
#. Airflow runs on a Redhat based system.

If this is not the case, appropriate changes will need to be made.

Please **note** that environment configuration is picked up from ``/etc/sysconfig/airflow``.

An example file is supplied within ``scripts/systemd``.
You can also define configuration at ``AIRFLOW_HOME`` or ``AIRFLOW_CONFIG``.
