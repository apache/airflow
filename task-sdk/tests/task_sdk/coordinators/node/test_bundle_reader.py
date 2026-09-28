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

from __future__ import annotations

import io
import json
import os
import pathlib
from unittest import mock

import pytest
from task_sdk.coordinators.node._bundle_test_utils import (
    BUNDLE_NAME,
    LAYOUT_PREFIX,
    METADATA_PREFIX,
    OFFSET_WIDTH,
    SCHEMA_VERSION,
    SOURCE_CLOSE,
    SOURCE_OPEN,
    metadata_json as _metadata_json,
    mutate_byte as _mutate_byte,
    read_layout as _read_layout,
    replace_layout_payload as _replace_layout_payload,
    rewrite_layout as _rewrite_layout,
    write_bundle,
)

from airflow.sdk.coordinators.node import _bundle_reader as _reader
from airflow.sdk.coordinators.node._bundle_reader import (
    _digest_cache,
    _hash_region,
    read_bundle,
    read_bundle_source,
)

from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

TYPESCRIPT_V1_FIXTURE = AIRFLOW_ROOT_PATH / "ts-sdk" / "tests" / "cli" / "fixtures" / "bundle-v1.min.mjs"


@pytest.fixture(autouse=True)
def clear_digest_cache():
    _digest_cache.clear()


class TestBundleReader:
    def test_reads_bundle_produced_by_typescript_encoder(self):
        assert "version" not in _read_layout(TYPESCRIPT_V1_FIXTURE)
        metadata = read_bundle(TYPESCRIPT_V1_FIXTURE)

        assert metadata.dag_ids == frozenset({"test_dag"})
        assert metadata.supervisor_schema_version == SCHEMA_VERSION

    def test_reads_source_embedded_by_typescript_encoder(self):
        # Real encoder output, whose source region needs both escape branches. Recovering it
        # exactly is the cross-language agreement on the escape scheme, which the Python helper
        # used by the other tests cannot establish on its own.
        assert read_bundle_source(TYPESCRIPT_V1_FIXTURE) == (
            "/** Handlers for the test Dag. */\n"
            'import { Bundle, Dag } from "apache-airflow-ts-sdk";\n'
            "\n"
            r"const TERMINATOR = /\*\//;"
            "\n"
            'const dag = new Dag("test_dag");\n'
            'dag.task("test_task", async () => TERMINATOR.source);\n'
            "\n"
            "await new Bundle(dag).serve();\n"
        )

    def test_accepts_block_comment_terminator_inside_the_code_region(self):
        # Rejecting a terminator in the code region would make every real bundle unreadable.
        fixture = TYPESCRIPT_V1_FIXTURE.read_bytes()
        layout = _read_layout(TYPESCRIPT_V1_FIXTURE)
        code_start = int(layout["code"]["start"], 16)  # type: ignore[index, call-overload]
        assert b"*/" in fixture[code_start:]

        assert read_bundle(TYPESCRIPT_V1_FIXTURE).dag_ids == frozenset({"test_dag"})

    def test_rejects_metadata_first_legacy_bundle(self, tmp_path):
        payload = _metadata_json("sales")
        (tmp_path / BUNDLE_NAME).write_bytes(METADATA_PREFIX + payload + b"\nexport {};\n")

        with pytest.raises(ValueError, match="no airflow bundle layout"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        "metadata_version",
        [None, "banana", "١.٠", "2.0", pytest.param(f"{'9' * 5_000}.0", id="unbounded-major")],
    )
    def test_rejects_unsupported_metadata_version(self, tmp_path, metadata_version):
        write_bundle(tmp_path, "sales", metadata_version=metadata_version)

        with pytest.raises(ValueError, match="unsupported airflow bundle metadata version"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_accepts_newer_minor_version_with_unknown_optional_fields(self, tmp_path):
        payload = json.loads(_metadata_json("sales", metadata_version="1.7.3"))
        payload["future_metadata_field"] = {"enabled": True}
        bundle = write_bundle(tmp_path, metadata_payload=json.dumps(payload).encode())
        layout = _read_layout(bundle)
        original_header_size = len(bundle.read_bytes().partition(b"\n")[0])
        layout["future_header_field"] = True
        layout["code"]["future_section_field"] = True  # type: ignore[index]
        # Fixed-width offsets let us relocate every section after extending the header.
        header_growth = len(LAYOUT_PREFIX) + len(json.dumps(layout).encode()) - original_header_size
        for name in ("metadata", "source", "code"):
            for field in ("start", "end"):
                value = int(layout[name][field], 16) + header_growth  # type: ignore[index, call-overload]
                layout[name][field] = f"{value:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _replace_layout_payload(bundle, json.dumps(layout).encode())

        metadata = read_bundle(bundle)

        assert metadata.dag_ids == frozenset({"sales"})
        assert metadata.supervisor_schema_version == SCHEMA_VERSION

    @pytest.mark.parametrize(
        ("payload", "message"),
        [
            (b"not-json", "cannot parse embedded airflow bundle layout"),
            (b"[]", "bundle layout must contain a mapping"),
        ],
    )
    def test_rejects_malformed_layout(self, tmp_path, payload, message):
        bundle = write_bundle(tmp_path, "sales")
        _replace_layout_payload(bundle, payload)

        with pytest.raises(ValueError, match=message):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize("encoding", ["utf-16", "utf-32"])
    def test_rejects_layout_that_is_not_utf8(self, tmp_path, encoding):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        payload = json.dumps(layout).encode(encoding)
        _replace_layout_payload(bundle, payload)

        with pytest.raises(ValueError, match="cannot parse embedded airflow bundle layout"):
            read_bundle(bundle)

    def test_rejects_oversized_layout(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        _replace_layout_payload(bundle, b"A" * _reader._MAX_LAYOUT_LINE_BYTES)

        with pytest.raises(ValueError, match="bundle layout exceeds"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_unterminated_layout(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout_line = bundle.read_bytes().partition(b"\n")[0]
        bundle.write_bytes(layout_line)

        with pytest.raises(ValueError, match="bundle layout is not newline-terminated"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_requires_metadata_immediately_after_layout(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        contents = bundle.read_bytes().replace(METADATA_PREFIX, b"//# notMetadata=", 1)
        bundle.write_bytes(contents)

        with pytest.raises(ValueError, match="no embedded airflow metadata after its layout"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_unterminated_metadata(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout_line, metadata_line, _ = bundle.read_bytes().split(b"\n", maxsplit=2)
        bundle.write_bytes(layout_line + b"\n" + metadata_line)

        with pytest.raises(ValueError, match="embedded airflow metadata is not newline-terminated"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        "metadata_payload",
        [
            _metadata_json("sales").replace(b"{", b"{\r", 1),
            _metadata_json("sales").replace(b"sales", "sales\u2028dag".encode(), 1),
            _metadata_json("sales").replace(b"sales", "sales\u2029dag".encode(), 1),
        ],
        ids=["carriage-return", "line-separator", "paragraph-separator"],
    )
    def test_rejects_metadata_with_javascript_line_terminator(self, tmp_path, metadata_payload):
        bundle = write_bundle(tmp_path, metadata_payload=metadata_payload)

        with pytest.raises(ValueError, match="metadata contains a JavaScript line terminator"):
            read_bundle(bundle)

    @pytest.mark.parametrize("section", ["code", "metadata", "source"])
    def test_requires_every_layout_section(self, tmp_path, section):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        del layout[section]
        _replace_layout_payload(bundle, json.dumps(layout).encode())

        with pytest.raises(ValueError, match=f"missing the {section} section"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_invalid_section_digest(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        layout["code"]["sha256"] = "z" * 64  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="64 lowercase hexadecimal digits"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_malformed_offset(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        layout["code"]["start"] = "Z" * OFFSET_WIDTH  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="16-digit lowercase hexadecimal"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(("start", "end"), [(1, 1), (2, 1)])
    def test_rejects_empty_or_reversed_section(self, tmp_path, start, end):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        layout["code"]["start"] = f"{start:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        layout["code"]["end"] = f"{end:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="code section must contain at least one byte"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_metadata_offset_mismatch(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        metadata_start = int(layout["metadata"]["start"], 16)  # type: ignore[index, call-overload]
        layout["metadata"]["start"] = f"{metadata_start + 1:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="metadata offsets do not match"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        "section", [pytest.param("metadata", id="metadata-before-decode"), "code", "source"]
    )
    def test_rejects_section_digest_mismatch(self, tmp_path, section):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        # For metadata, corrupt the opening brace so decoding would also fail if attempted first.
        _mutate_byte(bundle, int(layout[section]["start"], 16))  # type: ignore[index, call-overload]

        with pytest.raises(ValueError, match=f"{section} SHA-256 mismatch"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_source_offset_mismatch(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        source_start = int(layout["source"]["start"], 16)  # type: ignore[index, call-overload]
        layout["source"]["start"] = f"{source_start + 1:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="source offsets do not match"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_requires_source_comment_immediately_after_metadata(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        contents = bundle.read_bytes()
        # Replace the block-comment opener with a line comment of the same length,
        # leaving every declared offset intact.
        bundle.write_bytes(contents.replace(SOURCE_OPEN, b"//# airflowSourc\n", 1))

        with pytest.raises(ValueError, match="no embedded airflow source after its metadata"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_unterminated_source(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        contents = bundle.read_bytes()
        bundle.write_bytes(contents.replace(SOURCE_CLOSE, b"\n#*-\n", 1))

        with pytest.raises(ValueError, match="not closed by its block comment"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_source_declared_past_end_of_file(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        source_end = int(layout["source"]["end"], 16)  # type: ignore[index, call-overload]
        layout["source"]["end"] = f"{source_end + 4096:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="not closed by its block comment"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_oversized_source(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        layout = _read_layout(bundle)
        source_start = int(layout["source"]["start"], 16)  # type: ignore[index, call-overload]
        layout["source"]["end"] = f"{source_start + 1024 * 1024 + 1:0{OFFSET_WIDTH}x}"  # type: ignore[index]
        _rewrite_layout(bundle, layout)

        with pytest.raises(ValueError, match="embedded airflow source exceeds"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_unescaped_block_comment_terminator_in_source(self, tmp_path):
        # Node would end the comment at the terminator and execute what follows, while both
        # digests still match. Reject rather than vouch for such a bundle.
        write_bundle(tmp_path, "sales", source_payload=b'const s = "*/";')

        with pytest.raises(ValueError, match="unescaped block comment terminator"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_source_that_is_not_utf8(self, tmp_path):
        write_bundle(tmp_path, "sales", source_payload=b"const s = '" + bytes([0xFF]) + b"';")

        with pytest.raises(ValueError, match="embedded airflow source is not valid UTF-8"):
            read_bundle_source(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(b"export {};" + b"\n", id="plain"),
            pytest.param(b"/** doc */" + b"\nexport {};\n", id="doc-comment"),
            pytest.param(b'const s = "*/";\n', id="terminator"),
            pytest.param(b'const s = "*\\/";' + b"\n", id="escaped-slash"),
            pytest.param(b'const s = "*\\\\";' + b"\n", id="double-backslash"),
            pytest.param('const s = "\u65e5\u672c\u8a9e";\n'.encode(), id="non-ascii"),
            pytest.param(b"line one\nline two\nline three\n", id="multi-line"),
        ],
    )
    def test_round_trips_embedded_source_exactly(self, tmp_path, source):
        bundle = write_bundle(tmp_path, "sales", source=source)

        assert read_bundle_source(bundle).encode() == source

    def test_rejects_truncated_code(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        bundle.write_bytes(bundle.read_bytes()[:-1])

        with pytest.raises(ValueError, match="code offsets do not match"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_reports_truncation_while_hashing(self, tmp_path):
        with pytest.raises(ValueError, match="was truncated while hashing its code region"):
            _hash_region(io.BytesIO(), start=0, end=1, path=tmp_path / BUNDLE_NAME, section="code")

    def test_rejects_invalid_metadata_json_after_verification(self, tmp_path):
        write_bundle(tmp_path, "sales", metadata_payload=b"not-json")

        with pytest.raises(ValueError, match="cannot parse embedded airflow metadata"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        "metadata_payload",
        [
            b'{"value":"\xff"}',
            _metadata_json("sales").decode().encode("utf-16"),
        ],
        ids=["invalid-utf8", "utf16"],
    )
    def test_rejects_metadata_that_is_not_utf8(self, tmp_path, metadata_payload):
        write_bundle(tmp_path, "sales", metadata_payload=metadata_payload)

        with pytest.raises(ValueError, match="cannot parse embedded airflow metadata"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_metadata_that_is_not_a_mapping(self, tmp_path):
        write_bundle(tmp_path, "sales", metadata_payload=b"[]")

        with pytest.raises(ValueError, match="embedded airflow metadata must contain a mapping"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize("task_handlers", [None, []], ids=["missing", "not-a-mapping"])
    def test_rejects_missing_or_malformed_task_handlers(self, tmp_path, task_handlers):
        metadata = json.loads(_metadata_json("sales"))
        if task_handlers is None:
            del metadata["task_handlers"]
        else:
            metadata["task_handlers"] = task_handlers
        write_bundle(tmp_path, metadata_payload=json.dumps(metadata).encode())

        with pytest.raises(ValueError, match="metadata must contain a task_handlers mapping"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_oversized_metadata(self, tmp_path):
        write_bundle(
            tmp_path,
            "sales",
            metadata_payload=b"A" * _reader._MAX_METADATA_LINE_BYTES,
        )

        with pytest.raises(ValueError, match="embedded airflow metadata exceeds"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_change_during_verification(self, tmp_path, monkeypatch):
        bundle = write_bundle(tmp_path, "sales")
        original_hash_region = _hash_region

        def hash_then_touch(*args, **kwargs):
            digest = original_hash_region(*args, **kwargs)
            if kwargs["section"] == "metadata":
                stat_result = bundle.stat()
                os.utime(bundle, ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 1_000_000_000))
            return digest

        monkeypatch.setattr(_reader, "_hash_region", hash_then_touch)

        with pytest.raises(ValueError, match="changed while its integrity was being verified"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_rejects_change_after_verification_before_metadata_decode(self, tmp_path, monkeypatch):
        bundle = write_bundle(tmp_path, "sales")
        original_put = _digest_cache.put

        def put_then_touch(*args, **kwargs):
            original_put(*args, **kwargs)
            stat_result = bundle.stat()
            os.utime(bundle, ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 1_000_000_000))

        monkeypatch.setattr(_digest_cache, "put", put_then_touch)

        with pytest.raises(ValueError, match="changed while it was being read"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @pytest.mark.parametrize(
        ("failure_call", "message"),
        [
            (1, "cannot read bundle.min.mjs"),
            (2, "cannot stat bundle.min.mjs after verification"),
            (3, "cannot stat bundle.min.mjs after reading"),
        ],
    )
    def test_translates_fstat_errors(self, tmp_path, monkeypatch, failure_call, message):
        bundle = write_bundle(tmp_path, "sales")
        stat_result = bundle.stat()
        call_count = 0

        def fail_selected_fstat(_):
            nonlocal call_count
            call_count += 1
            if call_count == failure_call:
                raise OSError("test failure")
            return stat_result

        monkeypatch.setattr(_reader.os, "fstat", fail_selected_fstat)

        with pytest.raises(OSError, match=message):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_translates_metadata_read_error(self, tmp_path, monkeypatch):
        bundle = write_bundle(tmp_path, "sales")
        layout_line = bundle.read_bytes().splitlines(keepends=True)[0]
        bundle_file = mock.MagicMock(spec=io.BufferedReader)
        bundle_file.fileno.return_value = 1
        bundle_file.readline.side_effect = [layout_line, OSError("test failure")]
        path_open = mock.create_autospec(pathlib.Path.open)
        path_open.return_value = bundle_file
        monkeypatch.setattr(pathlib.Path, "open", path_open)
        monkeypatch.setattr(_reader.os, "fstat", lambda _: bundle.stat())

        with pytest.raises(OSError, match="cannot read bundle.min.mjs"):
            read_bundle(tmp_path / BUNDLE_NAME)

    @mock.patch.object(_reader, "_hash_region", autospec=True)
    def test_reuses_cached_digests_for_unchanged_bundle(self, hash_region, tmp_path):
        write_bundle(tmp_path, "sales")
        hash_region.side_effect = _hash_region

        read_bundle(tmp_path / BUNDLE_NAME)
        read_bundle(tmp_path / BUNDLE_NAME)

        # Three regions hashed on the first read, none on the second.
        assert hash_region.call_count == 3

    @mock.patch.object(_reader.os, "fstat", autospec=True)
    def test_cache_uses_ctime_to_detect_corruption_with_restored_mtime(self, fstat, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        original_stat = bundle.stat()
        # Control the reported timestamps rather than relying on filesystem clock resolution.
        fstat.return_value = mock.Mock(
            spec=os.stat_result,
            st_dev=original_stat.st_dev,
            st_ino=original_stat.st_ino,
            st_mtime_ns=original_stat.st_mtime_ns,
            st_ctime_ns=original_stat.st_ctime_ns,
            st_size=original_stat.st_size,
        )
        read_bundle(bundle)
        layout = _read_layout(bundle)
        _mutate_byte(bundle, int(layout["code"]["start"], 16))  # type: ignore[index, call-overload]
        fstat.return_value.st_ctime_ns += 1

        with pytest.raises(ValueError, match="code SHA-256 mismatch"):
            read_bundle(tmp_path / BUNDLE_NAME)

    def test_digest_cache_evicts_least_recently_used_entry(self):
        cache = _reader._BundleDigestCache(maxsize=2)
        section = _reader._DeclaredSection(start=0, end=1, sha256=b"0" * 32)
        digests = _reader._ComputedDigests(metadata=b"1" * 32, source=b"2" * 32, code=b"3" * 32)

        def build_key(inode):
            return _reader._DigestCacheKey(
                path="bundle.min.mjs",
                metadata=section,
                source=section,
                code=section,
                device=1,
                inode=inode,
                mtime_ns=1,
                ctime_ns=1,
                size=1,
            )

        cache.put(build_key(1), digests)
        cache.put(build_key(2), digests)
        assert cache.get(build_key(1)) == digests
        cache.put(build_key(3), digests)

        assert cache.get(build_key(2)) is None
        assert cache.get(build_key(1)) == digests
        assert cache.get(build_key(3)) == digests
