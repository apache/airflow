# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import
from __future__ import unicode_literals

from airflow.hooks.base_hook import BaseHook


class KmsApiHook(BaseHook):
    """
    Abstract base class for KMS hooks. KMS hooks should support encryption
    and decryption services. In addition, a KMS hook should support encrypting
    and decrypting connection keys.
    """

    def encrypt_conn_key(self, connection):
        """
        Accepts a Connection object and sets `connection.conn_key` by
        encrypting `connection._plain_conn_key` via the KMS.
        """
        raise NotImplementedError()

    def decrypt_conn_key(self, connection):
        """
        Accepts a Connection object and sets `connection._plain_conn_key` by
        decrypting `connection.conn_key` via the KMS.
        """
        raise NotImplementedError()
