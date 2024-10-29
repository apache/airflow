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

This files contains diagrams that help visualize what metrics are send, and what they represent.

## Dag execution metrics

```mermaid
---
displayMode: compact
---
gantt
  title Airflow metrics for a Dag run
  dateFormat HH:mm
  axisFormat %H:%M
  tickInterval 1hour

  section Events
    %% Milestones are "point in time" events, but they still take a duration.
    %% Setting it to 2 minutes here, I don't think it has any importance.

    Dag Scheduled         : milestone, dag_sched,   12:00, 2min
    Dag Starts            : milestone, dag_start,   13:00, 2min
    First task Scheduled  : milestone, task1_sched, 14:00, 2min
    Task N Scheduled      : milestone, taskN_sched, 15:00, 2min
    Task N starts running : milestone, taskN_start, 16:00, 2min
    Task N done           : milestone, taskN_done,  17:00, 2min
    Last task ends, Dag execution ends : milestone, end, 18:00, 2min

  section Metrics
    %% The start of the metrics can be conveniently marked with `after`,
    %% but with this kind of diagram, we have no way to "bind" the end to a milestone,
    %% so the durations have to be computed manually, sorry.
    %%
    %% If you modify the events above, make sure with your eyes that those metrics still match what they're supposed to.

    dagrun.schedule_delay           : after dag_sched, 1h
    first_task_scheduling_delay     : after dag_sched, 2h
    duration.success/failure.dag_id : after dag_start, 5h
    task_id.duration                : after taskN_start, 1h
    task N landing time (only in airflow UI) : after dag_sched, 5h
```
