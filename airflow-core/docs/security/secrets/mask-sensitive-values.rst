 .. Licensed to the Apache Software Foundation (ASF) under one
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    with the License.  You may obtain a copy of the License at


 ..   http://www.apache.org/licenses/LICENSE-2.0
 ..   http://www.apache.org/licenses/LICENSE-2.0


 .. Unless required by applicable law or agreed to in writing,
 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    specific language governing permissions and limitations
    under the License.
    under the License.


.. _security:mask-sensitive-values:
.. _security:mask-sensitive-values:


Masking sensitive data
Masking sensitive data
----------------------
----------------------


Airflow will by default mask Connection passwords and sensitive Variables and keys from a Connection's
Airflow will by default mask Connection passwords, sensitive Variables, and keys from a Connection's ``extra`` (JSON) field **only when the key name contains one or more of the default sensitive keywords**. Keys in the ``extra`` JSON that do not include any of these sensitive keywords will not be redacted automatically when rendered in logs, templates, or UI views.

**Default sensitive keywords**
``access_token``, ``api_key``, ``apikey``, ``authorization``, ``passphrase``, ``passwd``, ``password``, ``private_key``, ``secret``, ``token``, ``keyfile_dict``, ``service_account``.

**Examples of masking behavior**

.. list-table::
   :header-rows: 1

   * - Source
     - Key / Variable Name
     - Matching Keyword
     - Masking Scope
   * - Connection ``extra``
     - ``extra__google_cloud_platform__keyfile_dict``
     - ``keyfile_dict``
     - Everywhere (Rendered templates, Logs, UI)
   * - Connection ``extra``
     - ``hello``
     - —
     - Not masked
   * - Variable
     - ``service_account``
     - ``service_account``
     - Everywhere (Rendered templates, Logs, UI)
   * - Variable
     - ``test_keyfile_dict``
     - ``keyfile_dict``
     - Variables UI only

.. note::
   Masking is keyword-dependent. If you need custom masking rules, please open a feature request describing your use case.

**How to preview locally**
1. See CONTRIBUTING.md for the canonical docs build steps. Typical flow:
   ``pip install -r docs/requirements.txt`` (or project requirements) and then run the docs dev server (e.g., ``make docs`` or the repo-specific command).
2. Open the page in your browser and verify the table and paragraph render correctly.

Airflow will by default mask Connection passwords and sensitive Variables and keys from a Connection's
extra (JSON) field when they appear in Task logs, in the Variable and in the Rendered fields views of the UI.
extra (JSON) field when they appear in Task logs, in the Variable and in the Rendered fields views of the UI.


It does this by looking for the specific *value* appearing anywhere in your output. This means that if you
It does this by looking for the specific *value* appearing anywhere in your output. This means that if you
have a connection with a password of ``a``, then every instance of the letter a in your logs will be replaced
have a connection with a password of ``a``, then every instance of the letter a in your logs will be replaced
with ``***``.
with ``***``.


To disable masking you can set :ref:`config:core__hide_sensitive_var_conn_fields` to false.
To disable masking you can set :ref:`config:core__hide_sensitive_var_conn_fields` to false.


The automatic masking is triggered by Connection or Variable access. This means that if you pass a sensitive
The automatic masking is triggered by Connection or Variable access. This means that if you pass a sensitive
value via XCom or any other side-channel it will not be masked when printed in the downstream task.
value via XCom or any other side-channel it will not be masked when printed in the downstream task.


Sensitive field names
Sensitive field names
"""""""""""""""""""""
"""""""""""""""""""""


When masking is enabled, Airflow will always mask the password field of every Connection that is accessed by a
When masking is enabled, Airflow will always mask the password field of every Connection that is accessed by a
task.
task.


It will also mask the value of a Variable, rendered template dictionaries, XCom dictionaries or the
It will also mask the value of a Variable, rendered template dictionaries, XCom dictionaries or the
field of a Connection's extra JSON blob if the name is in the list of known-sensitive fields (i.e. 'access_token',
field of a Connection's extra JSON blob if the name is in the list of known-sensitive fields (i.e. 'access_token',
'api_key', 'apikey', 'authorization', 'passphrase', 'passwd', 'password', 'private_key', 'secret' or 'token').
'api_key', 'apikey', 'authorization', 'passphrase', 'passwd', 'password', 'private_key', 'secret' or 'token').
This list can also be extended:
This list can also be extended:


.. code-block:: ini
.. code-block:: ini


    [core]
    [core]
    sensitive_var_conn_names = comma,separated,sensitive,names
    sensitive_var_conn_names = comma,separated,sensitive,names


Adding your own masks
Adding your own masks
"""""""""""""""""""""
"""""""""""""""""""""


If you want to mask an additional secret that is not already masked by one of the above methods, you can do it in
If you want to mask an additional secret that is not already masked by one of the above methods, you can do it in
your Dag file or operator's ``execute`` function using the ``mask_secret`` function. For example:
your Dag file or operator's ``execute`` function using the ``mask_secret`` function. For example:


.. code-block:: python
.. code-block:: python


    @task
    @task
    def my_func():
    def my_func():
        from airflow.sdk.log import mask_secret
        from airflow.sdk.log import mask_secret


        mask_secret("custom_value")
        mask_secret("custom_value")


        ...
        ...


or
or


.. code-block:: python
.. code-block:: python




    class MyOperator(BaseOperator):
    class MyOperator(BaseOperator):
        def execute(self, context):
        def execute(self, context):
            from airflow.sdk.log import mask_secret
            from airflow.sdk.log import mask_secret


            mask_secret("custom_value")
            mask_secret("custom_value")


            ...
            ...


The mask must be set before any log/output is produced to have any effect.
The mask must be set before any log/output is produced to have any effect.


NOT masking when using environment variables
NOT masking when using environment variables
""""""""""""""""""""""""""""""""""""""""""""
""""""""""""""""""""""""""""""""""""""""""""


When you are using some operators - for example :class:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
When you are using some operators - for example :class:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
you might be tempted to pass secrets via environment variables. This is very bad practice because the environment
you might be tempted to pass secrets via environment variables. This is very bad practice because the environment
variables are visible to anyone who has access to see the environment of the process - such secrets passed by
variables are visible to anyone who has access to see the environment of the process - such secrets passed by
environment variables will NOT be masked by Airflow.
environment variables will NOT be masked by Airflow.


If you need to pass secrets to the KubernetesPodOperator, you should use native Kubernetes secrets or
If you need to pass secrets to the KubernetesPodOperator, you should use native Kubernetes secrets or
use Airflow Connection or Variables to retrieve the secrets dynamically.
use Airflow Connection or Variables to retrieve the secrets dynamically.
