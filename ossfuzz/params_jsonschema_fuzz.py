#!/usr/bin/python3
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

import os
import sys

import atheris

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    from airflow.serialization.definitions.param import SerializedParam, SerializedParamsDict


def _gen_schema(fdp: atheris.FuzzedDataProvider, depth: int = 0) -> dict:
    if depth > 2:
        return {}

    schema_type = fdp.PickValueInList(
        [
            "string",
            "integer",
            "number",
            "boolean",
            "object",
            "array",
            "null",
        ]
    )
    schema: dict = {"type": schema_type}

    if schema_type == "string":
        schema["maxLength"] = fdp.ConsumeIntInRange(0, 256)
        if fdp.ConsumeBool():
            schema["minLength"] = fdp.ConsumeIntInRange(0, schema["maxLength"])
    elif schema_type in ("integer", "number"):
        minimum = fdp.ConsumeIntInRange(-1_000_000, 1_000_000)
        maximum = fdp.ConsumeIntInRange(minimum, minimum + 1_000_000)
        schema["minimum"] = minimum
        schema["maximum"] = maximum
    elif schema_type == "array":
        schema["maxItems"] = fdp.ConsumeIntInRange(0, 32)
        if fdp.ConsumeBool():
            schema["items"] = _gen_schema(fdp, depth + 1)
    elif schema_type == "object":
        properties: dict[str, dict] = {}
        for _ in range(fdp.ConsumeIntInRange(0, 8)):
            name = fdp.ConsumeString(16)
            if not name:
                continue
            properties[name] = _gen_schema(fdp, depth + 1)
        schema["properties"] = properties
    return schema


def _gen_value(fdp: atheris.FuzzedDataProvider, schema: dict, depth: int = 0):
    if depth > 2:
        return None

    schema_type = schema.get("type")
    if schema_type == "string":
        return fdp.ConsumeString(256)
    if schema_type == "integer":
        return fdp.ConsumeIntInRange(-1_000_000, 1_000_000)
    if schema_type == "number":
        return fdp.ConsumeFloat()
    if schema_type == "boolean":
        return fdp.ConsumeBool()
    if schema_type == "null":
        return None
    if schema_type == "array":
        max_items = schema.get("maxItems", 16)
        count = fdp.ConsumeIntInRange(0, min(max_items, 16))
        item_schema = schema.get("items", {})
        return [_gen_value(fdp, item_schema, depth + 1) for _ in range(count)]
    if schema_type == "object":
        props = schema.get("properties", {})
        out: dict[str, object] = {}
        for k, prop_schema in props.items():
            if fdp.ConsumeBool():
                out[k] = _gen_value(fdp, prop_schema, depth + 1)
        return out

    # Fallback: generate a small JSON-like value.
    choice = fdp.ConsumeIntInRange(0, 4)
    if choice == 0:
        return None
    if choice == 1:
        return fdp.ConsumeBool()
    if choice == 2:
        return fdp.ConsumeIntInRange(-1024, 1024)
    if choice == 3:
        return fdp.ConsumeString(128)
    return [fdp.ConsumeString(16) for _ in range(fdp.ConsumeIntInRange(0, 4))]


def TestInput(input_bytes: bytes):
    if len(input_bytes) > 4096:
        return

    fdp = atheris.FuzzedDataProvider(input_bytes)

    schema = _gen_schema(fdp)
    value = _gen_value(fdp, schema)

    try:
        param = SerializedParam(value, source=fdp.PickValueInList(["dag", "task", None]), **schema)
        _ = param.resolve()
        try:
            _ = param.resolve(raises=True)
        except Exception:
            pass
    except Exception:
        pass

    params_dict: dict[str, object] = {}
    for _ in range(fdp.ConsumeIntInRange(0, 16)):
        key = fdp.ConsumeString(16)
        if not key:
            continue
        if fdp.ConsumeBool():
            s = _gen_schema(fdp)
            v = _gen_value(fdp, s)
            params_dict[key] = SerializedParam(v, source=fdp.PickValueInList(["dag", "task", None]), **s)
        else:
            params_dict[key] = _gen_value(fdp, _gen_schema(fdp))

    try:
        params = SerializedParamsDict(params_dict)
        _ = params.dump()
        try:
            _ = params.validate()
        except Exception:
            pass
        _ = params.deep_merge(params_dict).dump()
    except Exception:
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

