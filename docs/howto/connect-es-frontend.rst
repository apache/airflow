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

Connecting your Elasticsearch Frontend to the Webserver
=======================================================

If you are using Elasticsearch to render your logs in airflow, and if the Elasticsearch
client also has a frontend client for viewing those logs (Eg: Kibana), then you might
want to redirect your users to view their logs in that UI instead of Airflow UI. Kibana
for example has a nice UI to filter and search all logs for specific errors, or even viewing
multiple logs (not just one task instance).

In that case, you can just provide the frontend client link as follows :

.. code-block:: bash

    [elasticsearch]
    elasticsearch_frontend = <Domain>/_plugin/kibana/app/kibana#/discover?_g=()&_a=(filters:!((query:(match:(log_id:(query:'{log_id}',type:phrase))))))

The implementation basically constructs the ``log_id`` using the config param ``elasticsearch_log_id_template``.
The implicit assumption is that the ``log_id`` in the Elasticsearch stack is the same as the one specified in the
variable above. This ``log_id`` is replaced in the template frontend URL provided.

NOTE: Don't prefix "https://" in your domain, the implementation does it for you.

If the frontend client is specified, the dialog for each task instance will also include a redirect
to the frontend client for an alternate view for the logs.
