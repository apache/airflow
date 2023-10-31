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
"""Main executable module."""
from __future__ import annotations

import os

import argcomplete

# The configuration module initializes and validates the conf object as a side effect the first
# time it is imported. If it is not imported before importing the settings module, the conf
# object will then be initted/validated as a side effect of it being imported in settings,
# however this can cause issues since those modules are very tightly coupled and can
# very easily cause import cycles in the conf init/validate code (since downstream code from
# those functions likely import settings).
# Therefore importing configuration early (as the first airflow import) avoids
# any possible import cycles with settings downstream.
from airflow import configuration
from airflow.cli import cli_parser
from airflow.configuration import write_webserver_configuration_if_needed


def main():
    conf = configuration.conf
    if conf.get("core", "security") == "kerberos":
        os.environ["KRB5CCNAME"] = conf.get("kerberos", "ccache")
        os.environ["KRB5_KTNAME"] = conf.get("kerberos", "keytab")
    parser = cli_parser.get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    if args.subcommand not in ["lazy_loaded", "version"]:
        # Here we ensure that the default configuration is written if needed before running any command
        # that might need it. This used to be done during configuration initialization but having it
        # in main ensures that it is not done during tests and other ways airflow imports are used
        from airflow.configuration import write_default_airflow_configuration_if_needed

        conf = write_default_airflow_configuration_if_needed()
        if args.subcommand in ["webserver", "internal-api", "worker"]:
            write_webserver_configuration_if_needed(conf)
    args.func(args)


if __name__ == "__main__":
    main()
