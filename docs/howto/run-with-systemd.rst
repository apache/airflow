..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Running Airflow with systemd
============================

Airflow can integrate with systemd based systems. This makes watching your
daemons easy as systemd can take care of restarting a daemon on failure.
In the ``scripts/systemd`` directory you can find unit files that
have been tested on Redhat based systems. You can copy those to
``/usr/lib/systemd/system``. It is assumed that Airflow will run under
``airflow:airflow``. If not (or if you are running on a non Redhat
based system) you probably need to adjust the unit files.

Environment configuration is picked up from ``/etc/sysconfig/airflow``.
An example file is supplied. Make sure to specify the ``SCHEDULER_RUNS``
variable in this file when you run the scheduler. You
can also define here, for example, ``AIRFLOW_HOME`` or ``AIRFLOW_CONFIG``.
