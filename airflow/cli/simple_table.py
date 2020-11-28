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

from rich.box import ASCII_DOUBLE_HEAD
from rich.table import Table


class SimpleTable(Table):
    """A rich Table with some default hardcoded for consistency."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.show_edge = kwargs.get("show_edge", False)
        self.pad_edge = kwargs.get("pad_edge", False)
        self.box = kwargs.get("box", ASCII_DOUBLE_HEAD)
        self.show_header = kwargs.get("show_header", False)
        self.title_style = kwargs.get("title_style", "bold green")
        self.title_justify = kwargs.get("title_justify", "left")
        self.caption = kwargs.get("caption", " ")

    def add_column(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        """Add a column to the table. We use different default"""
        kwargs["overflow"] = kwargs.get("overflow", None)  # to avoid truncating
        super().add_column(*args, **kwargs)
