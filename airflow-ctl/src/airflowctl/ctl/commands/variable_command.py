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
from __future__ import annotations

import json
import os
import sys

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyVariableBody,
    BulkCreateActionVariableBody,
    VariableBody,
)


@provide_api_client(kind=ClientKind.CLI)
def import_(args, api_client=NEW_API_CLIENT) -> list[str]:
    """Import variables from a given file."""
    success_message = "[green]Import successful! success: {success}[/green]"
    errors_message = "[red]Import failed! errors: {errors}[/red]"

    if not os.path.exists(args.file):
        rich.print(f"[red]Missing variable file: {args.file}")
        sys.exit(1)
    with open(args.file) as var_file:
        try:
            var_json = json.load(var_file)
        except json.JSONDecodeError:
            rich.print(f"[red]Invalid variable file: {args.file}")
            sys.exit(1)

    action_on_existence = BulkActionOnExistence(args.action_on_existing_key)
    vars_to_update = []
    for k, v in var_json.items():
        value, description = v, None
        if isinstance(v, dict) and v.get("value"):
            value, description = v["value"], v.get("description")

        vars_to_update.append(
            VariableBody(
                key=k,
                value=value,
                description=description,
            )
        )

    bulk_body = BulkBodyVariableBody(
        actions=[
            BulkCreateActionVariableBody(
                action="create",
                entities=vars_to_update,
                action_on_existence=action_on_existence,
            )
        ]
    )
    result = api_client.variables.bulk(variables=bulk_body)
    if result.create.errors:
        rich.print(errors_message.format(errors=result.create.errors))
        sys.exit(1)

    rich.print(success_message.format(success=result.create.success))
    return result.create.success
