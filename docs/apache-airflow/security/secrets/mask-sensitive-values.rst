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

.. _security:mask-sensitive-values:

Masking sensitive data
----------------------

Airflow will by default mask Connection passwords and sensitive Variables and keys from a Connection's
extra (JSON) field when they appear in Task logs, in the Variable and in the Rendered fields views of the UI.

It does this by looking for the specific *value* appearing anywhere in your output. This means that if you
have a connection with a password of ``a``, then every instance of the letter a in your logs will be replaced
with ``***``.

To disable masking you can set :ref:`config:core__hide_sensitive_var_conn_fields` to false.

The automatic masking is triggered by Connection or Variable access. This means that if you pass a sensitive
value via XCom or any other side-channel it will not be masked when printed in the downstream task.

Sensitive field names
"""""""""""""""""""""

When masking is enabled, Airflow will always mask the password field of every Connection that is accessed by a
task.

It will also mask the value of a Variable, rendered template dictionaries, XCom dictionaries or the
field of a Connection's extra JSON blob if the name contains
any words in ('access_token', 'api_key', 'apikey', 'authorization', 'passphrase', 'passwd',
'password', 'private_key', 'secret', 'token'). This list can also be extended:

.. code-block:: ini

    [core]
    sensitive_var_conn_names = comma,separated,sensitive,names

Adding your own masks
"""""""""""""""""""""

If you want to mask an additional secret that is not already masked by one of the above methods, you can do it in
your DAG file or operator's ``execute`` function using the ``mask_secret`` function. For example:

.. code-block:: python

    @task
    def my_func():
        from airflow.utils.log.secrets_masker import mask_secret

        mask_secret("custom_value")

        ...

or

.. code-block:: python


    class MyOperator(BaseOperator):
        def execute(self, context):
            from airflow.utils.log.secrets_masker import mask_secret

            mask_secret("custom_value")

            ...

The mask must be set before any log/output is produced to have any effect.

NOT masking when using environment variables
""""""""""""""""""""""""""""""""""""""""""""

When you are using some operators - for example :class:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
you might be tempted to pass secrets via environment variables. This is very bad practice because the environment
variables are visible to anyone who has access to see the environment of the process - such secrets passed by
environment variables will NOT be masked by Airflow.

If you need to pass secrets to the KubernetesPodOperator, you should use native Kubernetes secrets or
use Airflow Connection or Variables to retrieve the secrets dynamically.
