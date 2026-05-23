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

# Dutch (nl) Translation Guidelines

This directory contains the Dutch translation files for the Airflow UI.

## Terminology Selection

The Dutch translation follows these core principles:

1.  **Airflow Specific Terms**: Terms like `Dag`, `XCom`, and `Asset` are kept in English as they are widely recognized technical objects within the Airflow ecosystem. We use `Dag` (not `DAG`) to align with Airflow conventions.
2.  **User Address**: We use the informal **"je/jouw"** register. This makes the UI feel more modern and accessible, which is a common trend in technical software localized for the Dutch market.
3.  **Consistency in Translation**:
    - Based on maintainer consensus, `Task` is consistently translated to `taak` (or `Taak`) everywhere, rather than keeping the English term for technical objects.
4.  **Established UI Mappings**:
    - `Schedule` -> `Planning`
    - `Queue` -> `Wachtrij`
    - `State` -> `Status`
    - `Run` -> `Run` (kept in English as it's a standard term in data engineering)

## Consistency

All new translations should be verified against `common.json` to ensure they use the standard Dutch terms for buttons (e.g., `Opslaan`, `Annuleren`, `Verwijderen`) and states (e.g., `Lopend`, `Mislukt`, `Succesvol`).
