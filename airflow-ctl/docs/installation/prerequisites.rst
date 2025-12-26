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

Prerequisites
-------------

airflowctl is tested with:


The minimum memory required we recommend airflowctl to run with is 200MB, but the actual requirements depend
wildly on the deployment options you have.
The Keyring backend needs to be installed separately into your operating system. This will enhance security. See :doc:`/security` for more information.

Keyring Backend
'''''''''''''''
airflowctl uses keyring to store the API token securely. This ensures that the token is not stored in plain text and is only accessible to authorized users.

Recommended keyring backends are:

* `macOS Keychain <https://en.wikipedia.org/wiki/Keychain_%28software%29>`_
* `Freedesktop Secret Service <http://standards.freedesktop.org/secret-service/>`_ supports many DE including GNOME (requires `secretstorage <https://pypi.python.org/pypi/secretstorage>`_)
* `KDE4 & KDE5 KWallet <https://en.wikipedia.org/wiki/KWallet>`_ (requires `dbus <https://pypi.python.org/pypi/dbus-python>`_)
* `Windows Credential Locker <https://docs.microsoft.com/en-us/windows/uwp/security/credential-locker>`_

Third-Party Backends
====================

In addition to the backends provided by the core keyring package for
the most common and secure use cases, there
are additional keyring backend implementations available for other
use cases. Simply install them to make them available:

- `keyrings.cryptfile <https://pypi.org/project/keyrings.cryptfile>`_
    - Encrypted text file storage.
- `keyrings.alt <https://pypi.org/project/keyrings.alt>`_
    - "alternate", possibly-insecure backends, originally part of the core package, but available for opt-in.
- `gsheet-keyring <https://pypi.org/project/gsheet-keyring>`_
    - a backend that stores secrets in a Google Sheet. For use with `ipython-secrets <https://pypi.org/project/ipython-secrets>`_.
- `bitwarden-keyring <https://pypi.org/project/bitwarden-keyring/>`_
    - a backend that stores secrets in the `BitWarden <https://bitwarden.com/>`_ password manager.
- `onepassword-keyring <https://pypi.org/project/onepassword-keyring/>`_
    - a backend that stores secrets in the `1Password <https://1password.com/>`_ password manager.
- `sagecipher <https://pypi.org/project/sagecipher>`_
    - an encryption backend which uses the ssh agent protocol's signature operation to derive the cipher key.
- `keyrings.osx_keychain_keys <https://pypi.org/project/keyrings.osx-keychain-keys>`_
    - ``OSX keychain key-management``, for private, public, and symmetric keys.
- `keyring_pass.PasswordStoreBackend <https://github.com/nazarewk/keyring_pass>`_
    - Password Store (pass) backend for python's keyring
- `keyring_jeepney <https://pypi.org/project/keyring_jeepney>`__
    - a pure Python backend using the secret service ``DBus`` API for desktop Linux (requires ``keyring<24``).


Python Version Compatibility
----------------------------
``airflowctl`` is compatible with versions of Python 3.10 through Python 3.14.
Currently, Python 3.14 is not supported. Thanks for your understanding!
We will work on adding support for Python 3.14.

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Python Version
     - Supported
   * - 3.10
     - Yes
   * - 3.11
     - Yes
   * - 3.12
     - Yes
   * - 3.13
     - Yes
   * - 3.14
     - No
