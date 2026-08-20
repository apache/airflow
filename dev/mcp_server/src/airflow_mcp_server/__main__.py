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
"""Entry point: ``airflow-dev-mcp`` / ``python -m airflow_mcp_server``."""

from __future__ import annotations

import argparse

from airflow_mcp_server.server import mcp


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="airflow-dev-mcp", description=__doc__)
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="MCP transport to serve on (default: stdio).",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to in http mode (default: 127.0.0.1). Ignored for stdio.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Port to bind to in http mode (default: 8081). Ignored for stdio.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.transport == "http":
        mcp.run(transport="http", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
