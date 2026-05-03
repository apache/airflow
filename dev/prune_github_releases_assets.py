#!/usr/bin/env python3
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
#
# Maintain Apache Airflow GitHub Releases so that they comply with the
# ASF release-distribution policy: each in-scope release body ends with
# a disclaimer footer that names the project's official downloads page
# at airflow.apache.org as the authoritative source, with PyPI /
# ArtifactHub mentioned as convenience mirrors. The binary asset
# attachments are removed so users cannot accidentally take a
# non-authoritative copy from GitHub.
#
# What this script does:
#
#   1. inventory       — list every release on apache/airflow with the assets
#                        attached and whether the disclaimer footer is already
#                        present in the release body.
#   2. update-bodies   — for every in-scope release whose body does not yet
#                        carry the disclaimer footer, append one (separated
#                        from the existing body by a `---` rule).
#   3. delete-assets   — for every in-scope release that still has assets,
#                        delete them (irreversible).
#
# Default scope is every release whose tag is not a 1.x core Airflow release.
# That covers core 2.x / 3.x, the Helm chart releases (`helm-chart/X.Y.Z`),
# `airflow-ctl/X.Y.Z`, the legacy `upgrade-check/X.Y.Z` releases, and
# release candidates / betas of any of the above.
#
# Every mode is dry-run by default. Use `--apply` to actually mutate.
#
# Authentication: uses the local `gh` CLI's auth, so make sure
# `gh auth status` reports an account with permission to edit releases on
# `apache/airflow` before running with `--apply`.
#
# Usage:
#   uv run dev/prune_github_releases_assets.py inventory
#   uv run dev/prune_github_releases_assets.py update-bodies          # dry-run
#   uv run dev/prune_github_releases_assets.py update-bodies --apply
#   uv run dev/prune_github_releases_assets.py delete-assets          # dry-run
#   uv run dev/prune_github_releases_assets.py delete-assets --apply
#
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

REPO = "apache/airflow"
DISCLAIMER_MARKER = "The **only authoritative** source for"

console = Console()

CORE_BLOCK = """\
> [!IMPORTANT]
> The **only authoritative** source for Apache Airflow {version} release
> artifacts is the official downloads page at:
>
> * https://airflow.apache.org/docs/apache-airflow/{version}/installation/installing-from-sources.html
>
> That page lists the source tarballs, wheels, detached `.asc` signatures,
> and `.sha512` checksums published by the ASF, with instructions for
> verifying each — including the project's signing keys at
> https://downloads.apache.org/airflow/KEYS.
>
> The wheels on PyPI are convenience binaries that are byte-for-byte
> identical to the corresponding ASF release artifacts; the same `.asc`
> signatures and `.sha512` checksums apply to them.
>
> * **PyPI:** https://pypi.org/project/apache-airflow/{version}/
"""

HELM_BLOCK = """\
> [!IMPORTANT]
> The **only authoritative** source for the Apache Airflow Helm chart
> {version} release artifacts is the official downloads page at:
>
> * https://airflow.apache.org/docs/helm-chart/{version}/installing-helm-chart-from-sources.html
>
> That page lists the chart tarball, detached `.asc` signature,
> `.sha512` checksum, and `.prov` provenance file published by the ASF,
> with instructions for verifying each — including the project's
> signing keys at https://downloads.apache.org/airflow/KEYS.
>
> The chart on ArtifactHub is a convenience mirror that is
> byte-for-byte identical to the corresponding ASF release artifact;
> the same `.asc` signature and `.sha512` checksum apply to it.
>
> * **ArtifactHub:** https://artifacthub.io/packages/helm/apache-airflow/airflow/{version}
"""

CTL_BLOCK = """\
> [!IMPORTANT]
> The **only authoritative** source for apache-airflow-ctl {version}
> release artifacts is the official downloads page at:
>
> * https://airflow.apache.org/docs/apache-airflow-ctl/{version}/installation/installing-from-sources.html
>
> That page lists the source tarballs, wheels, detached `.asc` signatures,
> and `.sha512` checksums published by the ASF, with instructions for
> verifying each — including the project's signing keys at
> https://downloads.apache.org/airflow/KEYS.
>
> The wheels on PyPI are convenience binaries that are byte-for-byte
> identical to the corresponding ASF release artifacts; the same `.asc`
> signatures and `.sha512` checksums apply to them.
>
> * **PyPI:** https://pypi.org/project/apache-airflow-ctl/{version}/
"""

UPGRADE_CHECK_BLOCK = """\
> [!IMPORTANT]
> The **only authoritative** source for apache-airflow-upgrade-check
> {version} release artifacts is the ASF distribution at:
>
> * https://archive.apache.org/dist/airflow/upgrade-check/{version}/
>
> Verify signatures against the project's signing keys at
> https://downloads.apache.org/airflow/KEYS.
>
> The wheels on PyPI are convenience binaries that are byte-for-byte
> identical to the corresponding ASF release artifacts.
>
> * **PyPI:** https://pypi.org/project/apache-airflow-upgrade-check/{version}/
"""

TEMPLATES = {
    "core": CORE_BLOCK,
    "helm": HELM_BLOCK,
    "ctl": CTL_BLOCK,
    "upgrade-check": UPGRADE_CHECK_BLOCK,
}


@dataclass
class Release:
    id: int
    tag: str
    name: str
    body: str
    is_draft: bool
    is_prerelease: bool
    assets: list[dict]

    @property
    def kind(self) -> str:
        if self.tag.startswith("helm-chart/"):
            return "helm"
        if self.tag.startswith("airflow-ctl/"):
            return "ctl"
        if self.tag.startswith("upgrade-check/"):
            return "upgrade-check"
        if re.match(r"^[0-9]", self.tag):
            return "core"
        return "unknown"

    @property
    def version(self) -> str:
        return self.tag.split("/", 1)[1] if "/" in self.tag else self.tag

    @property
    def is_one_dot_x_core(self) -> bool:
        return self.kind == "core" and self.version.split(".", 1)[0] == "1"

    def in_scope(self) -> bool:
        return self.kind != "unknown" and not self.is_one_dot_x_core

    def disclaimer_block(self) -> str:
        return TEMPLATES[self.kind].format(version=self.version)

    def has_disclaimer(self) -> bool:
        return DISCLAIMER_MARKER in (self.body or "")[:2000]


def gh_api(path: str, method: str = "GET", body: dict | None = None, paginate: bool = False) -> str:
    cmd = ["gh", "api", "-X", method, path]
    if paginate:
        cmd.insert(2, "--paginate")
    proc = subprocess.run(
        cmd,
        input=json.dumps(body) if body is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"gh api {method} {path} failed: {proc.stderr.strip()}")
    return proc.stdout


def list_releases() -> list[Release]:
    raw = gh_api(f"repos/{REPO}/releases?per_page=100", paginate=True)
    # `gh api --paginate` concatenates page bodies; each page is a JSON array.
    # We decode them in sequence and flatten the result.
    out: list[dict] = []
    decoder = json.JSONDecoder()
    idx = 0
    raw = raw.strip()
    while idx < len(raw):
        while idx < len(raw) and raw[idx].isspace():
            idx += 1
        if idx >= len(raw):
            break
        obj, end = decoder.raw_decode(raw, idx)
        if isinstance(obj, list):
            out.extend(obj)
        idx = end
    return [
        Release(
            id=r["id"],
            tag=r["tag_name"],
            name=r["name"] or r["tag_name"],
            body=r["body"] or "",
            is_draft=r["draft"],
            is_prerelease=r["prerelease"],
            assets=r["assets"],
        )
        for r in out
    ]


def cmd_inventory(releases: list[Release], out_path: Path) -> None:
    rows = [
        {
            "tag": r.tag,
            "name": r.name,
            "kind": r.kind,
            "version": r.version,
            "in_scope": r.in_scope(),
            "is_draft": r.is_draft,
            "is_prerelease": r.is_prerelease,
            "assets": [a["name"] for a in r.assets],
            "asset_count": len(r.assets),
            "has_disclaimer": r.has_disclaimer(),
            "body_first_line": (r.body or "").splitlines()[0][:120] if r.body else "",
        }
        for r in releases
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, indent=2))

    in_scope = [r for r in releases if r.in_scope()]
    pending_body = [r for r in in_scope if not r.has_disclaimer()]
    pending_assets = [r for r in in_scope if r.assets]

    table = Table(title=f"GitHub Releases on {REPO}")
    table.add_column("Bucket")
    table.add_column("Releases", justify="right")
    table.add_column("Assets", justify="right")
    table.add_row("Total", str(len(releases)), str(sum(len(r.assets) for r in releases)))
    table.add_row("In scope (non-1.x)", str(len(in_scope)), str(sum(len(r.assets) for r in in_scope)))
    table.add_row("Need disclaimer", str(len(pending_body)), "—")
    table.add_row(
        "Have assets to delete", str(len(pending_assets)), str(sum(len(r.assets) for r in pending_assets))
    )
    console.print(table)
    console.print(f"[green]Wrote inventory:[/green] {out_path}")


def cmd_update_bodies(releases: list[Release], apply: bool) -> None:
    targets = [r for r in releases if r.in_scope() and not r.has_disclaimer()]
    console.print(f"Bodies to update: [bold]{len(targets)}[/bold]")
    for r in targets:
        existing = (r.body or "").rstrip()
        separator = "\n\n---\n\n" if existing else ""
        new_body = existing + separator + r.disclaimer_block()
        action = "[red]APPLY[/red]" if apply else "[yellow]DRY  [/yellow]"
        console.print(f"  {action} {r.tag:<35} kind={r.kind:<13} +{len(new_body) - len(r.body or '')} chars")
        if apply:
            gh_api(f"repos/{REPO}/releases/{r.id}", method="PATCH", body={"body": new_body})


def cmd_delete_assets(releases: list[Release], apply: bool) -> None:
    targets = [r for r in releases if r.in_scope() and r.assets]
    total_assets = sum(len(r.assets) for r in targets)
    console.print(
        f"Releases with assets to delete: [bold]{len(targets)}[/bold] "
        f"(total [bold]{total_assets}[/bold] assets)"
    )
    for r in targets:
        for asset in r.assets:
            action = "[red]APPLY[/red]" if apply else "[yellow]DRY  [/yellow]"
            console.print(f"  {action} {r.tag:<35} {asset['name']:<60} ({asset['size']:>10} B)")
            if apply:
                gh_api(
                    f"repos/{REPO}/releases/assets/{asset['id']}",
                    method="DELETE",
                )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prune apache/airflow GitHub release assets and prepend "
        "an authoritative-source disclaimer to release bodies.",
    )
    parser.add_argument(
        "mode",
        choices=["inventory", "update-bodies", "delete-assets"],
        help="What to do.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate. Without this flag every mode is a dry-run.",
    )
    parser.add_argument(
        "--inventory-out",
        default="release-inventory.json",
        help="Where to write the inventory JSON file (default: ./release-inventory.json).",
    )
    args = parser.parse_args()

    console.print(f"[blue]Loading releases from {REPO}...[/blue]")
    releases = list_releases()
    console.print(f"[blue]Loaded {len(releases)} releases.[/blue]")

    if args.mode == "inventory":
        cmd_inventory(releases, Path(args.inventory_out))
    elif args.mode == "update-bodies":
        cmd_update_bodies(releases, args.apply)
    elif args.mode == "delete-assets":
        cmd_delete_assets(releases, args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
