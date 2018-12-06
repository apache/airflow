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

import sys
import json
import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from knack.log import get_logger
logger = get_logger(__name__)


def is_valid_ssh_rsa_public_key(openssh_pubkey):
    # http://stackoverflow.com/questions/2494450/ssh-rsa-public-key-validation-using-a-regular-expression
    # A "good enough" check is to see if the key starts with the correct header.
    import struct
    try:
        from base64 import decodebytes as base64_decode
    except ImportError:
        # deprecated and redirected to decodebytes in Python 3
        from base64 import decodestring as base64_decode

    parts = openssh_pubkey.split()
    if len(parts) < 2:
        return False
    key_type = parts[0]
    key_string = parts[1]

    data = base64_decode(key_string.encode())  # pylint:disable=deprecated-method
    int_len = 4
    str_len = struct.unpack('>I', data[:int_len])[0]  # this should return 7
    return data[int_len:int_len + str_len] == key_type.encode()


def get_public_key(self):
    key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
    return key.public_key().public_bytes(serialization.Encoding.OpenSSH,
                                         serialization.PublicFormat.OpenSSH).decode('utf-8')


def load_json(self, file_path):
    try:
        with open(file_path) as configFile:
            configData = json.load(configFile)
    except FileNotFoundError:
        print("Error: Expecting azurermconfig.json in current folder")
        sys.exit()
    return configData


def get_poller_result(self, poller, wait=5):
    '''
    Consistent method of waiting on and retrieving results from Azure's long poller
    :param poller Azure poller object
    :return object resulting from the original request
    '''
    try:
        delay = wait
        while not poller.done():
            logger.info("Waiting for {0} sec".format(delay))
            poller.wait(timeout=delay)
        return poller.result()
    except Exception as exc:
        logger.info("exception here {0} ".format(exc))
        raise


def get_default_dns_prefix(self, name, resource_group, subscription_id):
    # Use subscription id to provide uniqueness and prevent DNS name clashes
    name_part = re.sub('[^A-Za-z0-9-]', '', name)[0:10]
    if not name_part[0].isalpha():
        name_part = (str('a') + name_part)[0:10]
    resource_group_part = re.sub('[^A-Za-z0-9-]', '', resource_group)[0:16]
    return '{}-{}-{}'.format(name_part, resource_group_part, subscription_id[0:6])
