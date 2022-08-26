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

from pathlib import Path
from typing import Optional, Union


class DagProcessorDirectory:
    """Stores information about the dag directory used by the dag processor.

    Note that the value is available only for DagProcessorManager process.
    Any other user should read this value from e.g. DagModel.dag_directory field.
    """

    _dag_directory: Optional[str] = None

    @staticmethod
    def get_dag_directory() -> Optional[str]:
        return DagProcessorDirectory._dag_directory

    @staticmethod
    def set_dag_directory(value: Union[str, Path]):
        if value is Path:
            DagProcessorDirectory._dag_directory = str(Path(value).resolve())
        else:
            DagProcessorDirectory._dag_directory = str(value)
