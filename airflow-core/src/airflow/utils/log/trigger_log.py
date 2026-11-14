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
"""Where an asset watcher's own log lives, relative to the local and remote log folders."""

from __future__ import annotations

import posixpath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

TRIGGER_LOG_ROOT = "triggers"
# key=value directory names, as in the default task log path: "=" is in no Dag id or default-pattern run id,
# so a custom log_filename_template can't put a task's log where the orphan scan looks.
TRIGGER_ID_KEY = "trigger_id"
TRIGGERER_JOB_ID_KEY = "triggerer_job_id"
TRIGGER_LOG_FILE_NAME = "trigger.log"

# Older lines moved aside by the local size cap. Their own directory, because remote handlers with
# ``delete_local_logs`` delete the uploaded file's parent directory.
TRIGGER_LOG_ROTATED_DIR = "rotated"


def get_trigger_log_dir(trigger_id: int, producer_id: int | UUID | None = None) -> str:
    """Return a trigger's log directory, or with ``producer_id`` its directory on one triggerer."""
    trigger_dir = posixpath.join(TRIGGER_LOG_ROOT, f"{TRIGGER_ID_KEY}={trigger_id}")
    if producer_id is None:
        return trigger_dir
    return posixpath.join(trigger_dir, f"{TRIGGERER_JOB_ID_KEY}={producer_id}")


def parse_trigger_log_dir_id(key: str, dir_name: str) -> int | None:
    """Return the id in a ``<key>=<id>`` directory name, or None if it isn't one with a numeric id."""
    value = dir_name.removeprefix(f"{key}=")
    return int(value) if value != dir_name and value.isdigit() else None


def get_trigger_log_relative_path(trigger_id: int, producer_id: int | UUID) -> str:
    """
    Return a trigger's own log file on one triggerer.

    ``producer_id`` is the triggerer's job id (or its supervisor id without a job). One directory per
    triggerer, so uploading one triggerer's file never deletes another's.
    """
    return posixpath.join(get_trigger_log_dir(trigger_id, producer_id), TRIGGER_LOG_FILE_NAME)


def get_trigger_log_rotated_path(log_path: str) -> str:
    """Return where the local size cap moves the older lines of ``log_path``."""
    head, name = posixpath.split(log_path)
    return posixpath.join(head, TRIGGER_LOG_ROTATED_DIR, name)


def get_trigger_log_part_path(log_path: str, part: int) -> str:
    """
    Return where a remote handler that stores files uploads rotated file number ``part`` of ``log_path`` from.

    Each is its own remote object: to append to one, S3, GCS and WASB download and upload all of it again, and
    HDFS replaces it. Zero-padded, so name order, the order object stores list them in, is age order.
    """
    stem, ext = posixpath.splitext(get_trigger_log_rotated_path(log_path))
    return f"{stem}.{part:06d}{ext}"


def list_trigger_log_files(producer_dir: Path) -> list[Path]:
    """Return a watcher's files on one triggerer, oldest first: the rotated ones in name order, then the live one."""
    return [
        *sorted((producer_dir / TRIGGER_LOG_ROTATED_DIR).glob("*.log")),
        producer_dir / TRIGGER_LOG_FILE_NAME,
    ]
