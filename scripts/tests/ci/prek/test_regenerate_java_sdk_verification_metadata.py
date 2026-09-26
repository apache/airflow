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

import pathlib

import pytest
import regenerate_java_sdk_verification_metadata as regenerator
from regenerate_java_sdk_verification_metadata import (
    LICENSE_HEADER,
    METADATA,
    RegenerationFailedError,
    build_empty_metadata,
    insert_license_header,
    regenerate,
)

COMMITTED = f"""<?xml version="1.0" encoding="UTF-8"?>
{LICENSE_HEADER}
<verification-metadata xmlns="https://schema.gradle.org/dependency-verification">
   <configuration>
      <verify-metadata>true</verify-metadata>
   </configuration>
   <components>
      <component group="org.example" name="superseded" version="1.0"/>
      <component group="org.example" name="current" version="2.0"/>
   </components>
</verification-metadata>
"""


def component_names(metadata: str) -> list[str]:
    return [line.split('name="')[1].split('"')[0] for line in metadata.splitlines() if "<component " in line]


class FakeGradle:
    """Stands in for `gradlew --write-verification-metadata`.

    Two behaviours matter: it only ever adds to the component list, and it rewrites the
    file without the ASF header. Dropping an entry is therefore something only the caller
    can arrange, by emptying the list first.
    """

    def __init__(self, metadata: pathlib.Path, failures: int = 0, keeps_header: bool = False):
        self.metadata = metadata
        self.failures = failures
        self.keeps_header = keeps_header
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        if self.calls <= self.failures:
            return False
        text = self.metadata.read_text()
        kept = [line for line in text.splitlines() if "<component " in line]
        if not any('name="current"' in line for line in kept):
            kept.append('      <component group="org.example" name="current" version="2.0"/>')
        header = [LICENSE_HEADER] if self.keeps_header else []
        self.metadata.write_text(
            "\n".join(
                [
                    '<?xml version="1.0" encoding="UTF-8"?>',
                    *header,
                    '<verification-metadata xmlns="https://schema.gradle.org/dependency-verification">',
                    "   <components>",
                    *kept,
                    "   </components>",
                    "</verification-metadata>",
                ]
            )
            + "\n"
        )
        return True


@pytest.fixture
def metadata(tmp_path) -> pathlib.Path:
    path = tmp_path / "verification-metadata.xml"
    path.write_text(COMMITTED)
    return path


class TestBuildEmptyMetadata:
    def test_empties_the_component_list(self):
        assert component_names(build_empty_metadata(COMMITTED)) == []

    def test_keeps_the_header_and_the_configuration(self):
        emptied = build_empty_metadata(COMMITTED)
        assert LICENSE_HEADER in emptied
        assert "<verify-metadata>true</verify-metadata>" in emptied


class TestInsertLicenseHeader:
    def test_inserts_the_header_after_the_xml_declaration(self):
        stripped = '<?xml version="1.0" encoding="UTF-8"?>\n<verification-metadata/>\n'
        assert insert_license_header(stripped).splitlines()[1] == LICENSE_HEADER.splitlines()[0]

    def test_leaves_an_existing_header_alone(self):
        assert insert_license_header(COMMITTED) == COMMITTED


class TestRegenerate:
    def test_drops_superseded_entries_and_restores_the_header(self, metadata):
        regenerate(metadata, FakeGradle(metadata))

        assert component_names(metadata.read_text()) == ["current"]
        assert metadata.read_text().splitlines()[1] == LICENSE_HEADER.splitlines()[0]

    def test_leaves_a_single_header_when_gradle_keeps_the_one_it_was_given(self, metadata):
        regenerate(metadata, FakeGradle(metadata, keeps_header=True))

        assert metadata.read_text().count("Licensed to the Apache Software Foundation") == 1

    def test_retries_a_failing_run_and_succeeds(self, metadata):
        gradle = FakeGradle(metadata, failures=2)

        regenerate(metadata, gradle, sleep=lambda _: None)

        assert gradle.calls == 3
        assert component_names(metadata.read_text()) == ["current"]

    def test_restores_the_committed_file_when_every_attempt_fails(self, metadata):
        gradle = FakeGradle(metadata, failures=99)

        with pytest.raises(RegenerationFailedError):
            regenerate(metadata, gradle, sleep=lambda _: None)

        assert metadata.read_text() == COMMITTED


def test_license_header_matches_the_committed_metadata():
    committed = METADATA.read_text().splitlines()
    start = committed.index("<!--")
    end = committed.index("-->")
    assert "\n".join(committed[start : end + 1]) == LICENSE_HEADER


class TestMain:
    def test_prints_the_advisory_when_the_metadata_changed(self, capsys, monkeypatch):
        monkeypatch.setattr(regenerator, "regenerate", lambda *_: None)
        monkeypatch.setattr(regenerator, "metadata_changed", lambda _: True)

        assert regenerator.main() == 0
        assert "The trust list changed" in capsys.readouterr().err

    def test_stays_quiet_when_the_metadata_is_unchanged(self, capsys, monkeypatch):
        monkeypatch.setattr(regenerator, "regenerate", lambda *_: None)
        monkeypatch.setattr(regenerator, "metadata_changed", lambda _: False)

        assert regenerator.main() == 0
        assert capsys.readouterr().err == ""

    def test_reports_failure_when_no_attempt_succeeds(self, capsys, monkeypatch):
        def never_succeeds(*_):
            raise RegenerationFailedError("Regeneration failed after 3 attempts")

        monkeypatch.setattr(regenerator, "regenerate", never_succeeds)

        assert regenerator.main() == 1
        assert "Regeneration failed after 3 attempts" in capsys.readouterr().err
