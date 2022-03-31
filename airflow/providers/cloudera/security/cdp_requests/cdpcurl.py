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

"""cdpcurl implementation."""
import datetime
import pprint
import sys
from email.utils import formatdate

from airflow.providers.cloudera.security import submit_request
from airflow.providers.cloudera.security.cdp_requests.cdpv1sign import make_signature_header

__author__ = "cloudera"

IS_VERBOSE = False


def __log(*args, **kwargs):
    if not IS_VERBOSE:
        return
    stderr_pp = pprint.PrettyPrinter(stream=sys.stderr)
    stderr_pp.pprint(*args, **kwargs)


def __now():
    return datetime.datetime.now(datetime.timezone.utc)


# pylint: disable=too-many-arguments,too-many-locals
def make_request(method, uri, headers, data, access_key, private_key, data_binary, verify=True):
    """
    # Make HTTP request with CDP request signing

    :return: http request object
    :param method: str
    :param uri: str
    :param headers: dict
    :param data: str
    :param profile: str
    :param access_key: str
    :param private_key: str
    :param data_binary: bool
    :param verify: bool
    """

    if "x-altus-auth" in headers:
        raise Exception("x-altus-auth found in headers!")
    if "x-altus-date" in headers:
        raise Exception("x-altus-date found in headers!")
    headers["x-altus-date"] = formatdate(timeval=__now().timestamp(), usegmt=True)
    headers["x-altus-auth"] = make_signature_header(method, uri, headers, access_key, private_key)

    if data_binary:
        return __send_request(uri, data, headers, method, verify)
    return __send_request(uri, data.encode("utf-8"), headers, method, verify)


def __send_request(uri, data, headers, method, verify):
    __log("\nHEADERS++++++++++++++++++++++++++++++++++++")
    __log(headers)

    __log("\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++")
    __log("Request URL = " + uri)

    response = submit_request(method, uri, headers=headers, data=data, verify=verify)

    __log("\nRESPONSE++++++++++++++++++++++++++++++++++++")
    __log("Response code: %d\n" % response.status_code)

    return response
