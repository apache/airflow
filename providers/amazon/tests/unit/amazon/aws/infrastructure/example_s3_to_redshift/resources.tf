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

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "s3" {
  bucket = var.s3_bucket
  force_destroy = true
}

resource "aws_redshift_cluster" "redshift" {
  cluster_identifier = var.redshift_cluster_identifier
  database_name = var.redshift_database_name
  master_username = var.redshift_master_username
  master_password = var.redshift_master_password
  node_type = "dc1.large"
  cluster_type = "single-node"
  skip_final_snapshot = true
}
