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



Amazon Redshift Operators
=========================

Amazon offers two ways to query Redshift.

1. `Using Python connector <https://docs.aws.amazon.com/redshift/latest/mgmt/python-connect-examples.html>`__
2. `Using the Amazon Redshift Data API <https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html>`__

Airflow enables both. You can choose with what you want to work.

To use with API (HTTP) choose Amazon Redshift Data.

To use with Postgres Connection choose Amazon Redshift SQL.

.. toctree::
    :maxdepth: 1
    :glob:

    *
