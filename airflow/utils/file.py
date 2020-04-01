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
import io
import logging
import os
import re
import zipfile
from typing import Dict, List, Optional, Pattern

from airflow.configuration import conf

log = logging.getLogger(__name__)


def TemporaryDirectory(*args, **kwargs):  # pylint: disable=invalid-name
    """
    This function is deprecated. Please use `tempfile.TemporaryDirectory`
    """
    import warnings
    from tempfile import TemporaryDirectory as TmpDir
    warnings.warn(
        "This function is deprecated. Please use `tempfile.TemporaryDirectory`",
        DeprecationWarning, stacklevel=2
    )
    return TmpDir(*args, **kwargs)


def mkdirs(path, mode):
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :type path: str
    :param mode: The mode to give to the directory e.g. 0o755, ignores umask
    :type mode: int
    """
    try:
        o_umask = os.umask(0)
        os.makedirs(path, mode)
    except OSError:
        if not os.path.isdir(path):
            raise
    finally:
        os.umask(o_umask)


ZIP_REGEX = re.compile(r'((.*\.zip){})?(.*)'.format(re.escape(os.sep)))


def correct_maybe_zipped(fileloc):
    """
    If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive and path to zip is returned.
    """

    _, archive, _ = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    else:
        return fileloc


def open_maybe_zipped(fileloc, mode='r'):
    """
    Opens the given file. If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return zipfile.ZipFile(archive, mode=mode).open(filename)
    else:
        return io.open(fileloc, mode=mode)


def list_py_file_paths(directory: str,
                       safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE', fallback=True),
                       include_examples: Optional[bool] = None):
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :type directory: unicode
    :param safe_mode: whether to use a heuristic to determine whether a file
        contains Airflow DAG definitions. If not provided, use the
        core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
        to safe.
    :type safe_mode: bool
    :param include_examples: include example DAGs
    :type include_examples: bool
    :return: a list of paths to Python files in the specified directory
    :rtype: list[unicode]
    """
    if include_examples is None:
        include_examples = conf.getboolean('core', 'LOAD_EXAMPLES')
    file_paths: List[str] = []
    if directory is None:
        return []
    elif os.path.isfile(directory):
        return [directory]
    elif os.path.isdir(directory):
        patterns_by_dir: Dict[str, List[Pattern[str]]] = {}
        for root, dirs, files in os.walk(directory, followlinks=True):
            patterns: List[Pattern[str]] = patterns_by_dir.get(root, [])
            ignore_file = os.path.join(root, '.airflowignore')
            if os.path.isfile(ignore_file):
                with open(ignore_file, 'r') as file:
                    # If we have new patterns create a copy so we don't change
                    # the previous list (which would affect other subdirs)
                    lines_no_comments = [COMMENT_PATTERN.sub("", line) for line in file.read().split("\n")]
                    patterns += [re.compile(line) for line in lines_no_comments if line]

            # If we can ignore any subdirs entirely we should - fewer paths
            # to walk is better. We have to modify the ``dirs`` array in
            # place for this to affect os.walk
            dirs[:] = [
                subdir
                for subdir in dirs
                if not any(p.search(os.path.join(root, subdir)) for p in patterns)
            ]

            # We want patterns defined in a parent folder's .airflowignore to
            # apply to subdirs too
            for subdir in dirs:
                patterns_by_dir[os.path.join(root, subdir)] = patterns.copy()

            find_dag_file_paths(file_paths, files, patterns, root, safe_mode)
    if include_examples:
        from airflow import example_dags
        example_dag_folder = example_dags.__path__[0]  # type: ignore
        file_paths.extend(list_py_file_paths(example_dag_folder, safe_mode, False))
    return file_paths


def find_dag_file_paths(file_paths, files, patterns, root, safe_mode):
    """Finds file paths of all DAG files."""
    for f in files:
        # noinspection PyBroadException
        try:
            file_path = os.path.join(root, f)
            if not os.path.isfile(file_path):
                continue
            _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
            if file_ext != '.py' and not zipfile.is_zipfile(file_path):
                continue
            if any([re.findall(p, file_path) for p in patterns]):
                continue

            if not might_contain_dag(file_path, safe_mode):
                continue

            file_paths.append(file_path)
        except Exception:  # pylint: disable=broad-except
            log.exception("Error while examining %s", f)


COMMENT_PATTERN = re.compile(r"\s*#.*")


def might_contain_dag(file_path, safe_mode):
    """Heuristic that guesses whether a Python file contains an Airflow DAG definition."""
    if safe_mode and not zipfile.is_zipfile(file_path):
        with open(file_path, 'rb') as dag_file:
            content = dag_file.read()
            return all([s in content for s in (b'DAG', b'airflow')])
    return True
