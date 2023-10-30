<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Helm Chart for Apache Airflow

[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/apache-airflow)](https://artifacthub.io/packages/search?repo=apache-airflow)

[Apache Airflow](https://airflow.apache.org/) is a platform to programmatically author, schedule and monitor workflows.

## Introduction

This chart will bootstrap an [Airflow](https://airflow.apache.org) deployment on a [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

## Requirements

- Kubernetes 1.25+ cluster
- Helm 3.0+
- PV provisioner support in the underlying infrastructure (optionally)

## Features

* Supported executors: ``LocalExecutor``, ``CeleryExecutor``, ``KubernetesExecutor``, ``LocalKubernetesExecutor``, ``CeleryKubernetesExecutor``
* Supported Airflow version: ``1.10+``, ``2.0+``
* Supported database backend: ``PostgreSQL``, ``MySQL``
* Autoscaling for ``CeleryExecutor`` provided by KEDA
* ``PostgreSQL`` and ``PgBouncer`` with a battle-tested configuration
* Monitoring:
   * StatsD/Prometheus metrics for Airflow
   * Prometheus metrics for PgBouncer
   * Flower
* Automatic database migration after a new deployment
* Administrator account creation during deployment
* Kerberos secure configuration
* One-command deployment for any type of executor. You don't need to provide other services e.g. Redis/Database to test the Airflow.

## Documentation

Full documentation for Helm Chart (latest **stable** release) lives [on the website](https://airflow.apache.org/docs/helm-chart/).

> Note: If you're looking for documentation for main branch (latest development branch): you can find it on [s.apache.org/airflow-docs/](http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/helm-chart/stable/index.html).
> Source code for documentation is in [../docs/helm-chart](https://github.com/apache/airflow/tree/main/docs/helm-chart)
>

## Contributing

Want to help build Apache Airflow? Check out our [contributing documentation](https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst).
