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

output "redshift_endpoint" {
  value = aws_redshift_cluster.redshift.endpoint
  description = "The redshift endpoint which is needed to create an airflow connection."
}

output "redshift_database_name" {
  value = aws_redshift_cluster.redshift.database_name
  description = "The redshift database name which is needed to create an airflow connection."
}

output "redshift_master_username" {
  value = aws_redshift_cluster.redshift.master_username
  description = "The redshift username which is needed to create an airflow connection."
  sensitive = true
}

output "redshift_master_password" {
  value = aws_redshift_cluster.redshift.master_password
  description = "The redshift password which is needed to create an airflow connection."
  sensitive = true
}
