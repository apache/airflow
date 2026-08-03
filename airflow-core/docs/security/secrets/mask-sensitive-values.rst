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

Airflow will by default mask Connection passwords, sensitive Variables, and keys from a Connection's
extra (JSON) field whose names contain one or more of the sensitive keywords when they appear in Task logs,
in the Variables UI, and in the Rendered fields views of the UI. Keys in the extra JSON that do not include
any of these sensitive keywords will not be redacted automatically.

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

It will also mask the value of an Airflow Variable, rendered template dictionaries, XCom dictionaries or the field of a Connection's extra JSON blob if the
Variable name or field name contains any of the known-sensitive keywords.

**Default Sensitive Keywords:**

``access_token``, ``api_key``, ``apikey``, ``authorization``, ``passphrase``, ``passwd``, ``password``,
``private_key``, ``secret``, ``token``, ``keyfile_dict``, ``service_account``.

This list can also be extended using the environment variable ``AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES``:

.. code-block:: ini

    [core]
    sensitive_var_conn_names = comma,separated,sensitive,names

**Examples of Masking Behavior:**

.. list-table::
   :header-rows: 1
   :widths: 20 25 20 35

   * - Source
     - Key / Variable Name
     - Matching Keyword
     - Masking Scope
   * - Connection Extra
     - google_keyfile_dict
     - keyfile_dict
     - Everywhere (Logs, Rendered Templates, UI)
   * - Connection Extra
     - hello
     - None
     - Not Masked
   * - Variable
     - service_account
     - service_account
     - Everywhere (Logs, Rendered Templates, UI)
   * - Variable
     - test_keyfile_dict
     - keyfile_dict
     - Variables UI Only

Adding your own masks
"""""""""""""""""""""

If you want to mask an additional secret that is not already masked by one of the above methods, you can do it in
your Dag file or operator's ``execute`` function using the ``mask_secret`` function. For example:

.. code-block:: python

    @task
    def my_func():
        from airflow.sdk.log import mask_secret

        mask_secret("custom_value")

        ...

or

.. code-block:: python


    class MyOperator(BaseOperator):
        def execute(self, context):
            from airflow.sdk.log import mask_secret

            mask_secret("custom_value")

            ...

The mask must be set before any log/output is produced to have any effect.

Content-based masking of well-known secret formats
""""""""""""""""""""""""""""""""""""""""""""""""""

.. versionadded:: 3.2.0

Registering secrets explicitly via ``mask_secret`` (or through Connections and Variables) only
covers values Airflow was told about. Credentials that end up in Task logs or Rendered fields via
other paths — a debug ``print`` of an environment variable, a stack trace containing a token, a
value pulled from an XCom — are not covered by that mechanism.

To catch those cases, Airflow can additionally scan every string that passes through the secrets
masker for a small, curated set of well-known credential formats and redact any match. The set
is intentionally narrow — each entry has a distinctive prefix so a match is overwhelmingly
likely to be a real credential:

* AWS access / session keys (``AKIA…``, ``ASIA…``)
* GitHub tokens (``ghp_…``, ``gho_…``, ``ghu_…``, ``ghs_…``, ``ghr_…``)
* Slack tokens (``xoxb-…``, ``xoxp-…``, ``xoxa-…``, ``xoxr-…``, ``xoxs-…``)
* Google API keys (``AIza…``)
* Stripe live keys (``sk_live_…``)
* PEM-encoded private key blocks (``-----BEGIN … PRIVATE KEY-----``)
* JSON Web Tokens with the standard ``eyJ…`` header + payload prefix

This is a **defense-in-depth** measure that complements, but does not replace, explicit masking:
formats with high false-positive rates (generic credit-card numbers, SSNs, email addresses) are
deliberately excluded, and matches only fire on values that actually flow through the masker.
Secrets you already know about should still be registered via ``mask_secret``.

The feature is opt-in because the regex scan runs on every string that passes through the
masker. Enable it in your Airflow config:

.. code-block:: ini

    [core]
    mask_secrets_content_patterns = True

or via the corresponding environment variable
``AIRFLOW__CORE__MASK_SECRETS_CONTENT_PATTERNS=True``.

When enabled, log records and redacted values containing e.g. ``AKIAIOSFODNN7EXAMPLE`` are
rewritten so that only ``***`` appears in the output, while the surrounding text is preserved.

NOT masking when using environment variables
""""""""""""""""""""""""""""""""""""""""""""

When you are using some operators - for example :class:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
you might be tempted to pass secrets via environment variables. This is very bad practice because the environment
variables are visible to anyone who has access to see the environment of the process - such secrets passed by
environment variables will NOT be masked by Airflow.

If you need to pass secrets to the KubernetesPodOperator, you should use native Kubernetes secrets or
use Airflow Connection or Variables to retrieve the secrets dynamically.
