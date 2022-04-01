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
"""
Console used by all processes. We are forcing colors and terminal output as Breeze is supposed
to be only run in CI or real development terminal - in both cases we want to have colors on.
"""
try:
    from rich.console import Console
    from rich.theme import Theme

    custom_theme = Theme({"info": "blue", "warning": "magenta", "error": "red"})
    console = Console(force_terminal=True, color_system="standard", width=180, theme=custom_theme)

except ImportError:
    # We handle the ImportError so that autocomplete works with just click installed
    custom_theme = None  # type: ignore[assignment]
    console = None  # type: ignore[assignment]
