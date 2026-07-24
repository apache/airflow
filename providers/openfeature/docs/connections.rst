
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



.. _howto/connection:openfeature:

OpenFeature Connection
======================

The OpenFeature connection declares which OpenFeature *provider* the
:class:`~airflow.providers.openfeature.hooks.openfeature.OpenFeatureHook` registers, so DAG code can
evaluate flags against any backend without changing.

Configuring the Connection
---------------------------

Extra (optional)
    A JSON object declaring the provider to register once per process:

    - ``provider_class`` — the dotted path of an ``openfeature.provider.AbstractProvider`` subclass.
    - ``provider_kwargs`` — keyword arguments passed to that provider's constructor.

    Example, using the bundled in-process fractional provider:

    .. code-block:: json

        {
          "provider_class": "airflow.providers.openfeature.providers.fractional.FractionalProvider",
          "provider_kwargs": {}
        }

    If ``provider_class`` is omitted (or the connection does not exist), the hook uses whatever
    OpenFeature provider is already registered in the process.
