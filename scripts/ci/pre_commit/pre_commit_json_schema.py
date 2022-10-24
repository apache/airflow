#!/usr/bin/env python
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

import argparse
import hashlib
import json
import os
import re
import sys

import requests
import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import extend, validator_for

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )

AIRFLOW_SOURCES_DIR = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)


def _cache_dir():
    """Return full path to the user-specific cache dir for this application"""
    path = os.path.join(AIRFLOW_SOURCES_DIR, ".build", "cache")
    os.makedirs(path, exist_ok=True)
    return path


def _gethash(string: str):
    hash_object = hashlib.sha256(string.encode())
    return hash_object.hexdigest()[:8]


def fetch_and_cache(url: str, output_filename: str):
    """Fetch URL to local cache and returns path."""
    cache_key = _gethash(url)
    cache_dir = _cache_dir()
    cache_metadata_filepath = os.path.join(cache_dir, "cache-metadata.json")
    cache_filepath = os.path.join(cache_dir, f"{cache_key}-{output_filename[:64]}")
    # Create cache directory
    os.makedirs(cache_dir, exist_ok=True)
    # Load cache metadata
    cache_metadata: dict[str, str] = {}
    if os.path.exists(cache_metadata_filepath):
        try:
            with open(cache_metadata_filepath) as cache_file:
                cache_metadata = json.load(cache_file)
        except json.JSONDecodeError:
            os.remove(cache_metadata_filepath)
    etag = cache_metadata.get(cache_key)

    # If we have a file and etag, check the fast path
    if os.path.exists(cache_filepath) and etag:
        res = requests.get(url, headers={"If-None-Match": etag})
        if res.status_code == 304:
            return cache_filepath

    # Slow patch
    res = requests.get(url)
    res.raise_for_status()

    with open(cache_filepath, "wb") as output_file:
        output_file.write(res.content)

    # Save cache metadata, if needed
    etag = res.headers.get("etag", None)
    if etag:
        cache_metadata[cache_key] = etag
        with open(cache_metadata_filepath, "w") as cache_file:
            json.dump(cache_metadata, cache_file)

    return cache_filepath


class _ValidatorError(Exception):
    pass


def load_file(file_path: str):
    """Loads a file using a serializer which guesses based on the file extension"""
    if file_path.lower().endswith(".json"):
        with open(file_path) as input_file:
            return json.load(input_file)
    elif file_path.lower().endswith(".yaml") or file_path.lower().endswith(".yml"):
        with open(file_path) as input_file:
            return yaml.safe_load(input_file)
    raise _ValidatorError("Unknown file format. Supported extension: '.yaml', '.json'")


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validates the file using JSON Schema specifications")
    parser.add_argument(
        "--enforce-defaults", action="store_true", help="Values must match the default in the schema"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--spec-file", help="The path to specification")
    group.add_argument("--spec-url", help="The URL to specification")

    parser.add_argument("file", nargs="+")

    return parser


def _process_files(validator, file_paths: list[str]):
    exit_code = 0
    for input_path in file_paths:
        print("Processing file: ", input_path)
        instance = load_file(input_path)
        for error in validator.iter_errors(instance):
            print(error)
            exit_code = 1
    return exit_code


def _create_validator(schema, enforce_defaults: bool):
    cls = validator_for(schema)
    cls.check_schema(schema)
    if enforce_defaults:
        cls = extend(cls, {"default": _default_validator})
    return cls(schema)


def _default_validator(validator, default, instance, schema):
    # We will also accept a "See values.yaml" default
    if default != instance and default != "See values.yaml":
        yield ValidationError(f"{instance} is not equal to the default of {default}")


def _load_spec(spec_file: str | None, spec_url: str | None):
    if spec_url:
        spec_file = fetch_and_cache(url=spec_url, output_filename=re.sub(r"[^a-zA-Z0-9]", "-", spec_url))
    if not spec_file:
        raise Exception(f"The {spec_file} was None and {spec_url} did not lead to any file loading.")
    with open(spec_file) as schema_file:
        schema = json.loads(schema_file.read())
    return schema


def main() -> int:
    """Main code"""
    parser = _get_parser()
    args = parser.parse_args()
    spec_url = args.spec_url
    spec_file = args.spec_file
    enforce_defaults = args.enforce_defaults

    schema = _load_spec(spec_file, spec_url)

    validator = _create_validator(schema, enforce_defaults)

    file_paths = args.file
    exit_code = _process_files(validator, file_paths)

    return exit_code


sys.exit(main())
