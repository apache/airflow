#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import sys
import tempfile

import jsonschema
import requests
from typing import Dict

import yaml
from jsonschema.validators import validator_for

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )


def _user_cache_dir(appname=None):
    """Return full path to the user-specific cache dir for this application"""
    if sys.platform == "win32":
        # Windows has a complex procedure to download the App Dir directory because this directory can be
        # changed in window registry, so we use temporary directory for cache
        path = os.path.join(tempfile.gettempdir(), appname)
    elif sys.platform == 'darwin':
        path = os.path.expanduser('~/Library/Caches')
    else:
        path = os.getenv('XDG_CACHE_HOME', os.path.expanduser('~/.cache'))
    path = os.path.join(path, appname)
    return path


def _gethash(string: str):
    hash_object = hashlib.sha256(string.encode())
    return hash_object.hexdigest()


def fetch_and_cache(url: str, output_filename: str):
    """Fetch URL to local cache and returns path."""
    cache_key = _gethash(url)
    cache_dir = _user_cache_dir("pre-commmit-json-schema")
    cache_metadata_filepath = os.path.join(cache_dir, "cache-metadata.json")
    cache_filepath = os.path.join(cache_dir, f"{cache_key}-{output_filename}")
    # Create cache directory
    os.makedirs(cache_dir, exist_ok=True)
    # Load cache metadata
    cache_metadata: Dict[str, str] = {}
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
    etag = res.headers.get('etag', None)
    if etag:
        cache_metadata[cache_key] = etag
        with open(cache_metadata_filepath, 'w') as cache_file:
            json.dump(cache_metadata, cache_file)

    return cache_filepath


class ValidatorError(Exception):
    pass


def load_file(file_path):
    if file_path.lower().endswith('.json'):
        with open(file_path) as input_file:
            return json.load(input_file)
    elif file_path.lower().endswith('.yaml') or file_path.lower().endswith('.yml'):
        with open(file_path) as input_file:
            return yaml.load(input_file)
    raise ValidatorError("Unknown file format. Supported extension: '.yaml', '.json'")


def validate_spec(validator, instance):
    error = jsonschema.exceptions.best_match(validator.iter_errors(instance))
    if error is not None:
        raise ValidatorError(str(error))


def get_parser():
    parser = argparse.ArgumentParser(description='Validates the file using JSON Schema specifications')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--spec-file', help="The path to specification")
    group.add_argument('--spec-url', help="The URL to specification")

    parser.add_argument('file', nargs='+')
    return parser


def process_files(validator, file_paths):
    exit_code = 0
    for input_path in file_paths:
        print("Processing file: ", input_path)
        try:
            instance = load_file(input_path)
            validate_spec(validator, instance)
        except ValidatorError as ex:
            print('Problem processing the file.')
            print(ex)
            exit_code = 1
    return exit_code


def create_validator(schema):
    cls = validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


def load_spec(spec_file, spec_url):
    if spec_url:
        spec_file = fetch_and_cache(
            url=spec_url,
            output_filename=re.sub(r"[^a-zA-Z0-9]", "-", spec_url)
        )
    else:
        spec_file = spec_file
    with open(spec_file) as schema_file:
        schema = json.loads(schema_file.read())
    return schema


def main() -> int:
    parser = get_parser()
    args = parser.parse_args()
    spec_url = args.spec_url
    spec_file = args.spec_file

    schema = load_spec(spec_file, spec_url)

    validator = create_validator(schema)

    file_paths = args.file
    exit_code = process_files(validator, file_paths)

    return exit_code


sys.exit(main())
