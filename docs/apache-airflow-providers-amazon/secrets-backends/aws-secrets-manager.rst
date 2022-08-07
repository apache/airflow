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

AWS Secrets Manager Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable Secrets Manager, specify :py:class:`~airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``. These ``backend_kwargs`` are parsed as JSON, hence Python
values like the bool False or None will be ignored, taking for those kwargs the default values of the secrets backend.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables", "profile_name": "default", "full_url_mode": false}

To authenticate you can either supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config`` or set
environment variables like ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_DEFAULT_REGION``.


Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""
You can store the different values for a secret in two forms: storing the conn URI in one field (default mode) or using different
fields in Amazon Secrets Manager (setting ``full_url_mode`` as ``false`` in the backend config), as follows:

.. image:: /img/aws-secrets-manager.png

By default you must use some of the following words for each kind of field:

* For storing passwords, valid key names are password, pass and key
* Users: user, username, login, user_name
* Host: host, remote_host, server
* Port: port
* You should also specify the type of connection, which can be done naming the key as conn_type, conn_id,
  connection_type or engine. Valid values for this field are postgres, mysql, snowflake, google_cloud, mongo...
* For the extra value of the connections, a field called extra must exists. Please note this extra field
  should be a valid JSON.

However, more words can be added to the list using the parameter ``extra_conn_words`` in the configuration. This
parameter has to be a dict of lists with the following optional keys: user, password, host, schema, conn_type
As an example, if you have set ``connections_prefix`` as ``airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``airflow/connections/smtp_default``. This can be done through the AWS web
console or through Amazon CLI as shown below:

.. code-block:: bash

    aws secretsmanager put-secret-value \
        --secret-id airflow/connections/smtp_default \
        --secret-string '{"user": "nice_user","pass": "this_is_the_password","host": "ec2.8399.com","port": "999"}'

Verify that you can get the secret:

.. code-block:: console

    ❯ aws secretsmanager get-secret-value --secret-id airflow/connections/smtp_default
    {
        "ARN": "arn:aws:secretsmanager:us-east-2:314524341751:secret:airflow/connections/smtp_default-7meuul",
        "Name": "airflow/connections/smtp_default",
        "VersionId": "34f90eff-ea21-455a-9c8f-5ee74b21be672",
        "SecretString": "{\n  \"user\":\"nice_user\",\n  \"pass\":\"this_is_the_password\"\n,
        \n  \"host\":\"ec2.8399.com\"\n,\n  \"port\":\"999\"\n}\n",
        "VersionStages": [
            "AWSCURRENT"
        ],
        "CreatedDate": "2020-04-08T02:10:35.132000+01:00"
    }

If you don't want to use any ``connections_prefix`` for retrieving connections, set it as an empty string ``""`` in the configuration.

URL-Encoding of Secrets When Full URL Mode is False
"""""""""""""""""""""""""""""""""""""""""""""""""""

Previous versions of the Amazon provider package required values in the AWS secret to be URL-encoded when the setting ``full_url_mode`` is ``false``.
This behavior is now deprecated, and will be removed at a future date.

In most cases, you should not have any issues migrating your secrets to not being URL-encoded in advance of the deprecation.
Simply decoding your secret values will work, and no further changes are required.

In rare circumstances, the ``DeprecationWarning`` will tell you to add a new parameter to your ``backend_kwargs``.
This warning occurs when decoding is not idempotent.
A decoding is idempotent when decoding it once using the ``urllib.parse.unquote`` function is equivalent to decoding it two or more times using that function.
For example:

* If ``"foo%20bar"`` is a URL-encoded value, then decoding is idempotent because ``unquote(unquote("foo%20bar")) == unquote("foo%20bar")``
* If ``"foo%2520bar"`` is a URL-encoded value, then decoding is _not_ idempotent because ``unquote(unquote("foo%2520bar")) != unquote("foo%2520bar")``

Setting ``secret_values_are_urlencoded`` to ``false`` will force the ``SecretsManagerBackend`` to stop treating secret values as being URL-encoded.

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "full_url_mode": false, "secret_values_are_urlencoded": false}


Note that if ``full_url_mode`` is ``true``, it is still necessary to URL-encode the entire secret.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``airflow/variables/hello``.

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to AWS Secrets Manager for the excluded type.

If you want to look up some and not others in AWS Secrets Manager you may do so by setting the relevant ``*_prefix`` parameter of the ones to be excluded as ``null``.

For example, if you want to set parameter ``connections_prefix`` to ``"airflow/connections"`` and not look up variables, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": null, "profile_name": "default"}

Example of storing Google Secrets in AWS Secrets Manager
""""""""""""""""""""""""""""""""""""""""""""""""""""""""
For connecting to a google cloud conn, all the fields must be in the extra field, and their names follow the pattern
``extra_google_cloud_platform__value``. For example:

.. code-block:: ini

  {'extra__google_cloud_platform__key_path': '/opt/airflow/service_account.json',
  'extra__google_cloud_platform__scope': 'https://www.googleapis.com/auth/devstorage.read_only'}
