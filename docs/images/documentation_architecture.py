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

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.network import CloudFront
from diagrams.aws.storage import S3
from diagrams.custom import Custom
from diagrams.onprem.client import User, Users
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name


console = Console(width=400, color_system="standard")

GITHUB_LOGO = MY_DIR / "logos" / "github.png"
FASTLY_LOGO = MY_DIR / "logos" / "fastly.png"
ASF_LOGO = MY_DIR / "logos" / "asf_logo_wide.png"

graph_attr = {
    "concentrate": "false",
    "splines": "splines",
}

edge_attr = {
    "minlen": "1",
}


def generate_documentation_architecture_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")

    console.print(f"[bright_blue]Generating architecture image {image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        edge_attr=edge_attr,
        graph_attr=graph_attr,
    ):
        release_manager = User("Release Manager\n")
        committer = User("Committer")

        with Cluster("Airflow GitHub repos", graph_attr={"margin": "30"}):
            apache_airflow_repo = Custom("apache-airflow", GITHUB_LOGO.as_posix())
            apache_airflow_site_repo = Custom("apache-airflow-site", GITHUB_LOGO.as_posix())

            (
                apache_airflow_site_repo
                >> Edge(color="black", style="solid", label="Publish site (manual)\nMerge PR")
                >> committer
            )
            (
                apache_airflow_site_repo
                >> Edge(color="blue", style="solid", label="\n\n\nPull Theme (auto)")
                >> release_manager
            )
            (
                apache_airflow_repo
                >> Edge(
                    color="black",
                    style="solid",
                    label="Publish package docs (manual)\nPublish Docs to S3 Workflow",
                )
                >> release_manager
            )

        with Cluster("Live Docs", graph_attr={"margin": "80"}):
            live_bucket = S3("live-docs-airflow-apache-org")
            apache_airflow_site_archive_repo = Custom("apache-airflow-site-archive", GITHUB_LOGO.as_posix())
            apache_live_webserver = Custom("https://airflow.apache.org", ASF_LOGO.as_posix())
            cloudfront_live_cache = CloudFront("CloudFront Live: https://d7fnmbhf26p21.cloudfront.net")

            release_manager >> Edge(color="black", style="solid", label="Publish package docs") >> live_bucket
            (
                committer
                >> Edge(color="black", style="solid", label="Publish site\n\nInclude .htaccess proxy")
                >> apache_live_webserver
            )

            (
                live_bucket
                >> Edge(color="black", style="solid", label="Proxy docs from S3", minlen="2")
                >> cloudfront_live_cache
            )
            (
                cloudfront_live_cache
                >> Edge(color="black", style="solid", label="Cloudfront cache", minlen="2")
                >> apache_live_webserver
            )
            (
                live_bucket
                >> Edge(color="black", style="dashed", label="Archive docs (auto)", minlen="2")
                >> apache_airflow_site_archive_repo
            )

            (
                apache_airflow_site_archive_repo
                >> Edge(color="red", style="dotted", label="Fix docs (manual)", reverse=False, forward=False)
                >> live_bucket
            )

        fastly = Custom("Fastly CDN", FASTLY_LOGO.as_posix())
        users = Users("Users")
        apache_live_webserver >> Edge(color="black", style="solid", label="Exposed to Fastly") >> fastly
        fastly >> Edge(color="black", style="solid", label="Served to users") >> users

    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_documentation_architecture_diagram()
