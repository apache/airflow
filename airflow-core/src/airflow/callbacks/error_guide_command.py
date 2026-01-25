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
"""Error Guide Command."""

from __future__ import annotations

import logging

from airflow.cli.constants.error_guide import error_guide_dict
from airflow.cli.simple_table import AirflowConsole
from airflow.utils import cli as cli_utils

log = logging.getLogger(__name__)

template = """
The error you're facing would be with this message: "{error_message}".

As per our observations, a possible cause could be as follows:
{description}

To resolve this, as first step, you can try the following:
{first_steps}

If this doesn't resolve your problem, you can check out the docs for more info:
{documentation}

You may also ask your questions on the Airflow Slack #user-troubleshooting channel:
https://apache-airflow.slack.com/messages/user-troubleshooting

Happy Debugging! 🐞
"""


@cli_utils.action_cli
def show_error_guide(args):
    """Show the Error Guide."""
    console = AirflowConsole()
    if args.list_exceptions:
        exception_types: list[str] = list(
            sorted({payload["exception_type"] for _, payload in error_guide_dict.items()})
        )
        console.print_as(data=[{"Exception Type": item} for item in exception_types], output="table")
    elif args.error_code:
        error_code = str(args.error_code).upper()
        payload = error_guide_dict.get(error_code)
        if not payload:
            console.print(
                f"Invalid error code specified: '{error_code}'\n\n"
                "Please verify the error code, and if you're sure that it's correct, "
                "kindly report this as a bug through GitHub (https://github.com/apache/airflow/issues)"
            )
        else:
            console.print(template.format(**payload))
    elif args.list_guide:
        console.print_as_yaml(error_guide_dict)
    else:
        raise SystemExit("No arguments passed.")
