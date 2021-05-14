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


Changelog
---------


2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Due to licencing issues, the HTTP provider switched from ``requests`` to ``httpx`` library.
In case you use an authentication method different than default Basic Authenticaton,
you will need to change it to use the httpx-compatible one.

HttpHook's run() method passes any kwargs passed to underlying httpx.Requests object rather than
to requests.Requests. They largely compatible (for example json kwarg is supported in both) but not fully.
The ``content`` and ``auth`` parameters are not supported in ``httpx``.

The "verify", "proxie" and "cert" extra parameters

The get_conn() method of HttpHook returns ``httpx.Client`` rather than ``requests.Session``.

The get_conn() method of HttpHook has extra parameters: verify ,proxies and cert which are executed by the
run() method, so if your hook derives from the HttpHook it should be updated.

The ``run_and_check`` method is gone. PreparedRequests are not supported in ``httpx`` and this method
used it. Instead, run method simply creates and executes the full request instance.

While ``httpx`` does not support ``REQUESTS_CA_BUNDLE`` variable overriding ca crt, we backported
the requests behaviour to HTTPHook and it respects the variable if set. More details on that variable
can be found `here <https://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification`_


1.1.1
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Add a new argument for HttpSensor to accept a list of http status code``

1.0.0
.....

Initial version of the provider.
