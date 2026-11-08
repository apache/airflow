 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
    
Changelog
=========

Version 0.1.0
=============

Initial public release of the Hugging Face provider for Apache Airflow.

* Add ``HuggingFaceHook`` and the ``huggingface`` connection.
* Add ``LoadDatasetOperator``, ``MapDatasetOperator``,
  ``StreamDatasetOperator``, and ``PushToHubOperator``.
* Add revision pinned loads, Parquet artifacts, fingerprints, Hub publication
  commit metadata, and Hugging Face dataset assets.
* Set the artifact contract to JSON manifests and per-split Parquet objects.
  Direct streaming publication is included; standalone filtering and split
  assembly operators are not included in the v1 public API.
* Documented the minimum supported "datasets>=4.0.0,<5" version for Hub commit metadata support.
