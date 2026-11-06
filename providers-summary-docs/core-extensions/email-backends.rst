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

Email backends
---------------

This is a summary of all Apache Airflow Community provided implementations of email backends
exposed via community-managed providers.

Airflow can send email (for example, task success/failure/retry callbacks) through a pluggable
``email_backend`` instead of the default SMTP implementation, letting a provider send that email
through a third-party service's API.

You can read about the general email configuration mechanism in
:doc:`apache-airflow:howto/email-config` and here you can see the email backends
provided by the community-managed providers:

.. airflow-email-backends::
   :tags: None
   :header-separator: "
