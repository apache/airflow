#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
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

"""Main executable module"""
import os
import warnings

import argcomplete

from airflow import PY310
from airflow.cli import cli_parser
from airflow.configuration import conf
from airflow.utils.docs import get_docs_url


def main():
    """Main executable function"""
    if conf.get("core", "security") == 'kerberos':
        os.environ['KRB5CCNAME'] = conf.get('kerberos', 'ccache')
        os.environ['KRB5_KTNAME'] = conf.get('kerberos', 'keytab')
    if PY310:
        docs_url = get_docs_url('installation/prerequisites.html')
        warnings.warn(
            "Python v3.10 is not official supported on this version of Airflow. Please be careful. "
            f"For details, see: {docs_url}"
        )

    parser = cli_parser.get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
