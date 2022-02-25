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

# CI Sequence diagrams

You can see here the sequence diagrams of the flow happening during the CI Jobs.

## Pull request flow from fork

```mermaid
sequenceDiagram
    Note over Airflow Repo: pull request
    Note over Tests: pull_request<br>[Read Token]
    Note over Build Images: pull_request_target<br>[Write Token]
    activate Airflow Repo
    Airflow Repo -->> Tests: Trigger 'pull_request'
    activate Tests
    Tests -->> Build Images: Trigger 'pull_request_target'
    activate Build Images
    Note over Build Images: Build info
    par 3.6, [3.7, 3.8, 3.9]
        activate GitHub Registry
        GitHub Registry ->> Build Images: Pull CI Images from Cache
        deactivate GitHub Registry
        Note over Build Images: Build CI Images<br>[COMMIT_SHA]
    end
    par No CI image
        Note over Tests: Build info<br>Which tests?<br>Which Python?
    and
        Note over Tests: OpenAPI client gen
    and
        Note over Tests: Test UI
    and
        Note over Tests: Test examples<br>PROD image building
    end
    par 3.6, [3.7, 3.8, 3.9]
        activate GitHub Registry
        Build Images ->> GitHub Registry: Push CI Images
        Note over GitHub Registry: Tagged CI Images<br>[COMMIT_SHA]
    end
    par 3.6, [3.7, 3.8, 3.9]
        GitHub Registry ->> Build Images: Pull PROD Images from Cache
        Note over Build Images: Build PROD Images<br>[COMMIT_SHA]
    end
    loop Wait for CI images
        par 3.6, [3.7, 3.8, 3.9]
            Tests ->> Tests: Check CI Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, [3.7, 3.8, 3.9]
        GitHub Registry ->> Tests: Pull CI Image
        Note over Tests: Verify CI Image
    end
    deactivate GitHub Registry
    par 3.6, [3.7, 3.8, 3.9]
        opt Needed?
            Note over Tests: Run static checks
        end
    and
        opt Needed?
            Note over Tests: Run basic <br>static checks
        end
    and
        opt Needed?
            Note over Tests: Build docs
        end
    and
        opt Needed?
            Note over Tests: Tests
        end
    and
        opt Needed?
            Note over Tests: Test provider <br>packages build
        end
    and
        opt Needed?
            Note over Tests: Helm tests
        end
    end
    par 3.6, [3.7, 3.8, 3.9]
        Build Images ->> GitHub Registry: Push PROD Images
        activate GitHub Registry
    end
    deactivate Build Images
    Note over GitHub Registry: Tagged PROD Images<br>[COMMIT_SHA]
    loop Wait for PROD images
        par 3.6, [3.7, 3.8, 3.9]
            Tests ->> Tests: Check PROD Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, [3.7, 3.8, 3.9]
        GitHub Registry ->> Tests: Pull PROD Image
        Note over Tests: Verify PROD Image
    end
    deactivate GitHub Registry
    par 3.6, [3.7, 3.8, 3.9]
        opt Needed?
            Note over Tests: Run Kubernetes <br>tests
        end
    and
        opt Needed?
            Note over Tests: Run Kubernetes <br>upgrade tests
        end
    end
    Tests -->> Airflow Repo: Status update
    deactivate Airflow Repo
    deactivate Tests
```

## Direct Push/Merge flow

```mermaid
sequenceDiagram
    Note over Airflow Repo: merge
    Note over Tests: push<br>[Write Token]
    Note over Build Images: push<br>[Write Token]
    activate Airflow Repo
    Airflow Repo -->> Tests: Trigger 'push'
    activate Tests
    Airflow Repo -->> Build Images: Trigger 'push'
    activate Build Images
    Note over Build Images: Build info
    par 3.6, 3.7, 3.8, 3.9
        activate GitHub Registry
        GitHub Registry ->> Build Images: Pull CI Images from Cache
        deactivate GitHub Registry
        Note over Build Images: Build CI Images<br>[COMMIT_SHA]
    end
    par No CI image
        Note over Tests: Build info<br>All tests<br>All python
    and
        Note over Tests: OpenAPI client gen
    and
        Note over Tests: Test UI
    and
        Note over Tests: Test examples<br>PROD image building
    end
    par 3.6, 3.7, 3.8, 3.9
        Build Images ->> GitHub Registry: Push CI Images
        activate GitHub Registry
        Note over GitHub Registry: Tagged CI Images<br>[COMMIT_SHA]
    end
    par 3.6, 3.7, 3.8, 3.9
        GitHub Registry ->> Build Images: Pull PROD Images from Cache
        Note over Build Images: Build PROD Images<br>[COMMIT_SHA]
    end
    loop Wait for CI images
        par 3.6, 3.7, 3.8, 3.9
            Tests ->> Tests: Check CI Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, 3.7, 3.8, 3.9
        GitHub Registry ->> Tests: Pull CI Image [COMMIT_SHA]
        Note over Tests: Verify CI Image
    end
    deactivate GitHub Registry
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Run static checks
    and
        Note over Tests: Build docs
    and
        Note over Tests: Tests
    and
        Note over Tests: Test provider <br>packages build
    and
        Note over Tests: Helm tests
    end
    par 3.6, 3.7, 3.8, 3.9
        Build Images ->> GitHub Registry: Push PROD Images
        Note over GitHub Registry: Tagged PROD Images<br>[COMMIT_SHA]
        activate GitHub Registry
    end
    deactivate Build Images
    loop Wait for PROD images
        par 3.6, 3.7, 3.8, 3.9
            Tests ->> Tests: Check PROD Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, 3.7, 3.8, 3.9
        GitHub Registry ->> Tests: Pull PROD Image [COMMIT_SHA]
        Note over Tests: Verify PROD Image
    end
    deactivate GitHub Registry
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Run Kubernetes <br>tests
    and
        Note over Tests: Run Kubernetes <br>upgrade tests
    end
    Note over Tests: Merge Coverage
    Tests -->> Coverage.io: Upload Coverage
    par 3.6, 3.7, 3.8, 3.9
        Tests ->> GitHub Registry: Push CI Images to Cache
        activate GitHub Registry
    and
        Tests ->> GitHub Registry: Push PROD Images to Cache
    end
    Note over GitHub Registry: Tagged Images<br>[latest]
    deactivate GitHub Registry
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Generate constraints
        Tests ->> Airflow Repo: Push constraints
    end
    Tests -->> Airflow Repo: Status update
    deactivate Airflow Repo
    deactivate Tests
```

## Scheduled build flow

```mermaid
sequenceDiagram
    Note over Airflow Repo: scheduled
    Note over Tests: schedule<br>[Write Token]
    Note over Build Images: schedule<br>[Write Token]
    activate Airflow Repo
    Airflow Repo -->> Tests: Trigger 'schedule'
    activate Tests
    Airflow Repo -->> Build Images: Trigger 'schedule'
    activate Build Images
    Note over Build Images: Build info
    par 3.6, 3.7, 3.8, 3.9
        Note over Build Images: Build CI Images<br>Cache disabled<br>[COMMIT_SHA]
    end
    par No CI image
        Note over Tests: Build info<br>All tests<br>All python
    and
        Note over Tests: OpenAPI client gen
    and
        Note over Tests: Test UI
    and
        Note over Tests: Test examples<br>PROD image building
    end
    par 3.6, 3.7, 3.8, 3.9
        Build Images ->> GitHub Registry: Push CI Images
        activate GitHub Registry
        Note over GitHub Registry: Tagged CI Images<br>[COMMIT_SHA]
    end
    par 3.6, 3.7, 3.8, 3.9
        Note over Build Images: Build PROD Images<br>Cache disabled<br>[COMMIT_SHA]
    end
    loop Wait for CI images
        par 3.6, 3.7, 3.8, 3.9
            Tests ->> Tests: Check CI Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, 3.7, 3.8, 3.9
        GitHub Registry ->> Tests: Pull CI Image [COMMIT_SHA]
        Note over Tests: Verify CI Image
    end
    deactivate GitHub Registry
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Run static checks
    and
        Note over Tests: Build docs
    and
        Note over Tests: Tests
    and
        Note over Tests: Test provider <br>packages build
    and
        Note over Tests: Helm tests
    end
    par 3.6, 3.7, 3.8, 3.9
        Build Images ->> GitHub Registry: Push PROD Images
        activate GitHub Registry
        Note over GitHub Registry: Tagged PROD Images<br>[COMMIT_SHA]
    end
    deactivate Build Images
    loop Wait for PROD images
        par 3.6, 3.7, 3.8, 3.9
            Tests ->> Tests: Check PROD Images
            Note over Tests: Wait for<br>[COMMIT_SHA]
        end
    end
    par 3.6, 3.7, 3.8, 3.9
        GitHub Registry ->> Tests: Pull PROD Image [COMMIT_SHA]
        Note over Tests: Verify PROD Image
    end
    deactivate GitHub Registry
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Run Kubernetes <br>tests
    and
        Note over Tests: Run Kubernetes <br>upgrade tests
    end
    par 3.6, 3.7, 3.8, 3.9
        Note over Tests: Generate constraints
        Tests ->> Airflow Repo: Push constraints
    end
    Tests -->> Airflow Repo: Status update
    deactivate Airflow Repo
    deactivate Tests
```
