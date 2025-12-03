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

import json
import os
import subprocess
import sys
from functools import cached_property

import boto3

from airflow_breeze.global_constants import PACKAGES_METADATA_EXCLUDE_NAMES
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.parallel import check_async_run_results, run_with_pool

PROVIDER_NAME_FORMAT = "apache-airflow-providers-{}"

NON_SHORT_NAME_PACKAGES = ["apache-airflow", "apache-airflow-ctl", "docker-stack", "helm-chart", "task-sdk"]


s3_client = boto3.client("s3")
cloudfront_client = boto3.client("cloudfront")


class VersionError:
    """Class to track version errors during processing."""

    version_error: bool = False

    @staticmethod
    def has_any_error() -> bool:
        return VersionError.version_error

    @staticmethod
    def set_version_error(value: bool):
        VersionError.version_error = value


def get_cloudfront_distribution(destination_location):
    if "live-docs" in destination_location:
        return "E26P75MP9PMULE"
    return "E197MS0XRJC5F3"


class S3DocsPublish:
    def __init__(
        self,
        source_dir_path: str,
        destination_location: str,
        exclude_docs: str,
        dry_run: bool = False,
        overwrite: bool = False,
        parallelism: int = 1,
        skip_write_to_stable_folder: bool = False,
    ):
        self.source_dir_path = source_dir_path
        self.destination_location = destination_location
        self.exclude_docs = exclude_docs
        self.dry_run = dry_run
        self.overwrite = overwrite
        self.parallelism = parallelism
        self.source_dest_mapping: list[tuple[str, str]] = []
        self.skip_write_to_stable_folder = skip_write_to_stable_folder

    @cached_property
    def get_all_docs(self):
        get_console().print(f"[info]Getting all docs from {self.source_dir_path}\n")
        try:
            all_docs = os.listdir(self.source_dir_path)
        except FileNotFoundError:
            get_console().print(f"[error]No docs found in {self.source_dir_path}\n")
            sys.exit(1)
        return all_docs

    @cached_property
    def get_all_excluded_docs(self):
        if not self.exclude_docs:
            return []
        excluded_docs = self.exclude_docs.split(" ")

        # We remove `no-docs-excluded` string, this will be send from github workflows input as default value.
        if "no-docs-excluded" in excluded_docs:
            excluded_docs.remove("no-docs-excluded")
        return excluded_docs

    @cached_property
    def get_all_eligible_docs(self):
        """
        It excludes the docs that are in the exclude list
        """
        non_eligible_docs = []

        for excluded_doc in self.get_all_excluded_docs:
            if excluded_doc in NON_SHORT_NAME_PACKAGES:
                non_eligible_docs.append(excluded_doc)
                continue

            for doc in self.get_all_docs:
                excluded_provider_name = PROVIDER_NAME_FORMAT.format(excluded_doc.replace(".", "-"))
                if doc == excluded_provider_name:
                    non_eligible_docs.append(doc)
                    continue

        docs_to_process = list(set(self.get_all_docs) - set(non_eligible_docs))
        if not docs_to_process:
            get_console().print("[error]No eligible docs found, all docs are excluded\n")
            sys.exit(1)

        return docs_to_process

    def doc_exists(self, s3_bucket_doc_location: str) -> bool:
        bucket, key = self.get_bucket_key(s3_bucket_doc_location)
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

        return response["KeyCount"] > 0

    def sync_docs_to_s3(self, source: str, destination: str):
        if self.dry_run:
            get_console().print(f"Dry run enabled, skipping sync operation {source} to {destination}")
            return (0, "")
        get_console().print(f"[info]Syncing {source} to {destination}\n")
        result = subprocess.run(
            ["aws", "s3", "sync", "--delete", source, destination],
            check=False,
            capture_output=True,
            text=True,
        )
        return (result.returncode, result.stderr)

    def publish_stable_version_docs(self):
        """
        Publish stable version docs to S3. The source dir should have a stable.txt file and it
        publishes to two locations: one with the version folder and another with stable folder
        ex:
        docs/apache-airflow-providers-apache-cassandra/1.0.0
        docs/apache-airflow-providers-apache-cassandra/stable
        """

        for doc in self.get_all_eligible_docs:
            # PACKAGES_METADATA_EXCLUDE_NAMES has no stable versions so we copy them directly
            if doc not in PACKAGES_METADATA_EXCLUDE_NAMES:
                stable_file_path = f"{self.source_dir_path}/{doc}/stable.txt"
                if os.path.exists(stable_file_path):
                    with open(stable_file_path) as stable_file:
                        stable_version = stable_file.read()
                        get_console().print(f"[info]Stable version: {stable_version} for {doc}\n")
                else:
                    get_console().print(
                        f"[info]Skipping, stable version file not found for {doc} in {stable_file_path}\n"
                    )
                    continue

                dest_doc_versioned_folder = f"{self.destination_location}/{doc}/{stable_version}/"
                dest_doc_stable_folder = f"{self.destination_location}/{doc}/stable/"

                if self.doc_exists(dest_doc_versioned_folder):
                    if self.overwrite:
                        get_console().print(
                            f"[info]Overwriting existing version {stable_version} for {doc}\n"
                        )
                    else:
                        get_console().print(
                            f"[info]Skipping doc publish for {doc} as version {stable_version} already exists\n"
                        )
                        continue

                source_dir_doc_path = f"{self.source_dir_path}/{doc}/{stable_version}/"

                self.source_dest_mapping.append((source_dir_doc_path, dest_doc_versioned_folder))

                if not self.skip_write_to_stable_folder:
                    self.source_dest_mapping.append((source_dir_doc_path, dest_doc_stable_folder))
            else:
                source_dir_doc_path = f"{self.source_dir_path}/{doc}/"
                dest_doc_versioned_folder = f"{self.destination_location}/{doc}/"
                self.source_dest_mapping.append((source_dir_doc_path, dest_doc_versioned_folder))

        if self.source_dest_mapping:
            self.run_publish()

    def publish_all_docs(self):
        for doc in self.get_all_eligible_docs:
            dest_doc_folder = f"{self.destination_location}/{doc}/"
            if self.doc_exists(dest_doc_folder):
                if self.overwrite:
                    get_console().print(f"[info]Overwriting existing {dest_doc_folder}\n")
                else:
                    get_console().print(
                        f"[info]Skipping doc publish for {dest_doc_folder} as already exists\n"
                    )
                    continue

            source_dir_doc_path = f"{self.source_dir_path}/{doc}/"
            self.source_dest_mapping.append((source_dir_doc_path, dest_doc_folder))

        if self.source_dest_mapping:
            self.run_publish()

    def run_publish(self):
        all_params = [
            f"Publish docs from {source} to {destination}" for source, destination in self.source_dest_mapping
        ]

        with run_with_pool(
            parallelism=self.parallelism,
            all_params=all_params,
        ) as (pool, outputs):
            results = [
                pool.apply_async(
                    self.sync_docs_to_s3,
                    kwds={
                        "source": source,
                        "destination": destination,
                    },
                )
                for source, destination in self.source_dest_mapping
            ]

        check_async_run_results(
            results=results,
            success_message="All docs published successfully",
            outputs=outputs,
            include_success_outputs=False,
        )

        # Now generate the packages-metadata.json
        self.generate_packages_metadata()

        # Add redirects to package folders
        [
            self.add_redirect(destination)
            for _, destination in self.source_dest_mapping
            if destination.endswith("stable/")
        ]

    def list_s3_directories(self, s3_path: str) -> list[str]:
        """
        List 'directories' (prefixes) in an S3 path using boto3.
        """
        bucket, prefix = self.get_bucket_key(s3_path.rstrip("/"))
        paginator = s3_client.get_paginator("list_objects_v2")
        result = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/", Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                result.append(f"s3://{bucket}/" + cp["Prefix"])
        return result

    def generate_packages_metadata(self):
        get_console().print("[info]Generating packages-metadata.json file\n")

        if self.dry_run:
            get_console().print("Dry run enabled, skipping packages-metadata.json generation")
            return

        package_versions_map = {}
        s3_docs_path = self.destination_location.rstrip("/") + "/"
        resp = self.list_s3_directories(s3_docs_path)

        for package_path in resp:
            package_name = package_path.replace(s3_docs_path, "").rstrip("/")

            if package_name in PACKAGES_METADATA_EXCLUDE_NAMES:
                continue

            versions = [
                version_path.replace(package_path, "").rstrip("/")
                for version_path in self.list_s3_directories(package_path)
                if version_path.replace(package_path, "").rstrip("/") != "stable"
            ]
            package_versions_map[package_name] = versions

        all_packages_infos = self.dump_docs_package_metadata(package_versions_map)

        bucket, _ = self.get_bucket_key(self.destination_location)

        get_console().print("[info]Uploading packages-metadata.json to S3\n")
        s3_client.put_object(
            Bucket=bucket,
            Key="manifest/packages-metadata.json",
            Body=json.dumps(all_packages_infos, indent=2),
            ContentType="application/json",
        )
        get_console().print("[success]packages-metadata.json file generated successfully\n")
        distribution_id = get_cloudfront_distribution(self.destination_location)
        get_console().print(
            f"[info]Invalidating CloudFront cache for the uploaded files: distribution id {distribution_id}\n"
        )
        cloudfront_client.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                "Paths": {
                    "Quantity": 1,
                    "Items": ["/*"],
                },
                "CallerReference": str(int(os.environ.get("GITHUB_RUN_ID", str(0)))),
            },
        )
        get_console().print(
            f"[success]CloudFront cache request invalidated successfully: {distribution_id}\n"
        )

    def dump_docs_package_metadata(self, package_versions: dict[str, list[str]]):
        all_packages_infos = [
            {
                "package-name": package_name,
                "all-versions": (all_versions := self.get_latest_minor_versions(package_name, versions)),
                "stable-version": all_versions[-1],
            }
            for package_name, versions in package_versions.items()
        ]

        return all_packages_infos

    @staticmethod
    def get_latest_minor_versions(package_name: str, versions: list[str]) -> list[str]:
        from packaging.version import Version

        get_console().print(f"[info]Getting package versions for {package_name} from:\n")
        get_console().print(versions)
        all_versions: list[Version] = []
        for v in versions:
            try:
                all_versions.append(Version(v))
            except ValueError as e:
                get_console().print(f"[error]Invalid version {v}: {e}\n")
                VersionError.set_version_error(True)
        all_versions.sort(reverse=True)
        minor_versions: list[str] = []
        good_versions = []
        for version in all_versions:
            minor_version = str(version.major) + "." + str(version.minor)
            if minor_version not in minor_versions:
                get_console().print(f"[info]Latest minor version added: {version}\n")
                minor_versions.append(minor_version)
                good_versions.append(str(version))
            else:
                get_console().print(f"[info]Not latest minor version skipped: {version}\n")
        MAX_VERSIONS = 20
        selected_versions = good_versions[:MAX_VERSIONS][::-1]
        get_console().print(f"[info]Selected {MAX_VERSIONS} versions for {package_name}:\n")
        get_console().print(selected_versions)
        return selected_versions

    @staticmethod
    def get_bucket_key(bucket_path: str) -> tuple[str, str]:
        parts = bucket_path[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1]
        return bucket, key

    def add_redirect(self, path: str):
        """
        Add redirects for the docs to the S3 bucket
        ex: The redirect will be placed in the docs/{package}/index.html
        """
        bucket, key = self.get_bucket_key(path)

        redirect_path = f"/{key}index.html"
        s3_key = key.replace("stable/", "") + "index.html"

        get_console().print(f"[info]Adding redirect {redirect_path} in {s3_key}\n")

        html_body = f"""<!DOCTYPE html>
<html>
   <head><meta http-equiv="refresh" content="1; url={redirect_path}" /></head>
   <body></body>
</html>"""

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=html_body,
            ContentType="text/html",
        )
