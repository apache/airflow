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
"""Sanitizer for body fields sent via GCP API.

The sanitizer removes fields specified from the body.

Context
-------
In some cases where GCP operation requires modification of existing resources (such
as instances or instance templates) we need to sanitize body of the resources returned
via GCP APIs. This is in the case when we retrieve information from GCP first,
modify the body and either update the existing resource or create a new one with the
modified body. Usually when you retrieve resource from GCP you get some extra fields which
are Output-only, and we need to delete those fields if we want to use
the body as input for subsequent create/insert type operation.


Field specification
-------------------

Specification of fields is an array of strings which denote names of fields to be removed.
The field can be either direct field name to remove from the body or the full
specification of the path you should delete - separated with '.'


>>> FIELDS_TO_SANITIZE = [
>>>    "kind",
>>>    "properties.disks.kind",
>>>    "properties.metadata.kind",
>>>]
>>> body = {
>>>     "kind": "compute#instanceTemplate",
>>>     "name": "instance",
>>>     "properties": {
>>>         "disks": [
>>>             {
>>>                 "name": "a",
>>>                 "kind": "compute#attachedDisk",
>>>                 "type": "PERSISTENT",
>>>                 "mode": "READ_WRITE",
>>>             },
>>>             {
>>>                 "name": "b",
>>>                 "kind": "compute#attachedDisk",
>>>                 "type": "PERSISTENT",
>>>                 "mode": "READ_WRITE",
>>>             }
>>>         ],
>>>         "metadata": {
>>>             "kind": "compute#metadata",
>>>             "fingerprint": "GDPUYxlwHe4="
>>>         },
>>>     }
>>> }
>>> sanitizer=GcpBodyFieldSanitizer(FIELDS_TO_SANITIZE)
>>> sanitizer.sanitize(body)
>>> json.dumps(body, indent=2)
{
    "name":  "instance",
    "properties": {
        "disks": [
            {
                "name": "a",
                "type": "PERSISTENT",
                "mode": "READ_WRITE",
            },
            {
                "name": "b",
                "type": "PERSISTENT",
                "mode": "READ_WRITE",
            }
        ],
        "metadata": {
            "fingerprint": "GDPUYxlwHe4="
        },
    }
}

Note that the components of the path can be either dictionaries or arrays of dictionaries.
In case  they are dictionaries, subsequent component names key of the field, in case of
arrays - the sanitizer iterates through all dictionaries in the array and searches
components in all elements of the array.
"""

from typing import List

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class GcpFieldSanitizerException(AirflowException):
    """Thrown when sanitizer finds unexpected field type in the path
    (other than dict or array).
    """


class GcpBodyFieldSanitizer(LoggingMixin):
    """Sanitizes the body according to specification.

    :param sanitize_specs: array of strings that specifies which fields to remove
    :type sanitize_specs: list[str]

    """

    def __init__(self, sanitize_specs: List[str]) -> None:
        super().__init__()
        self._sanitize_specs = sanitize_specs

    def _sanitize(self, dictionary, remaining_field_spec, current_path):
        field_split = remaining_field_spec.split(".", 1)
        if len(field_split) == 1:  # pylint: disable=too-many-nested-blocks
            field_name = field_split[0]
            if field_name in dictionary:
                self.log.info("Deleted %s [%s]", field_name, current_path)
                del dictionary[field_name]
            else:
                self.log.debug(
                    "The field %s is missing in %s at the path %s.", field_name, dictionary, current_path
                )
        else:
            field_name = field_split[0]
            remaining_path = field_split[1]
            child = dictionary.get(field_name)
            if child is None:
                self.log.debug(
                    "The field %s is missing in %s at the path %s. ", field_name, dictionary, current_path
                )
            elif isinstance(child, dict):
                self._sanitize(child, remaining_path, "{}.{}".format(current_path, field_name))
            elif isinstance(child, list):
                for index, elem in enumerate(child):
                    if not isinstance(elem, dict):
                        self.log.warning(
                            "The field %s element at index %s is of wrong type. "
                            "It should be dict and is %s. Skipping it.",
                            current_path,
                            index,
                            elem,
                        )
                    self._sanitize(elem, remaining_path, "{}.{}[{}]".format(current_path, field_name, index))
            else:
                self.log.warning(
                    "The field %s is of wrong type. It should be dict or list and it is %s. Skipping it.",
                    current_path,
                    child,
                )

    def sanitize(self, body):
        """
        Sanitizes the body according to specification.
        """
        for elem in self._sanitize_specs:
            self._sanitize(body, elem, "")
