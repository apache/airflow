/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export type Edge = {
  is_setup_teardown?: boolean;
  label?: string;
  source_id: string;
  target_id: string;
};

export type Node = {
  children?: Array<Node>;
  id: string;
  is_mapped?: boolean;
  label: string;
  setup_teardown_type?: "setup" | "teardown";
  tooltip?: string;
  type:
    | "asset_alias"
    | "asset_condition"
    | "asset"
    | "dag"
    | "join"
    | "sensor"
    | "task"
    | "trigger";
};

export type GraphData = {
  arrange: "BT" | "LR" | "RL" | "TB";
  edges: Array<Edge>;
  nodes: Array<Node>;
};

export const graphData: GraphData = {
  arrange: "LR",
  edges: [
    {
      source_id: "section_1.upstream_join_id",
      target_id: "section_1.taskgroup_setup",
    },
    {
      source_id: "section_1.downstream_join_id",
      target_id: "section_2.upstream_join_id",
    },
    {
      source_id: "section_1.normal",
      target_id: "section_1.taskgroup_teardown",
    },
    {
      is_setup_teardown: true,
      label: "setup and teardown",
      source_id: "section_1.taskgroup_setup",
      target_id: "section_1.taskgroup_teardown",
    },
    {
      label: "test",
      source_id: "section_1.taskgroup_teardown",
      target_id: "section_1.downstream_join_id",
    },
    {
      source_id: "section_1.taskgroup_setup",
      target_id: "section_1.normal",
    },
    {
      source_id: "section_2.downstream_join_id",
      target_id: "end",
    },
    {
      source_id: "section_2.inner_section_2.task_2",
      target_id: "section_2.inner_section_2.task_4",
    },
    {
      source_id: "section_2.inner_section_2.task_3",
      target_id: "section_2.inner_section_2.task_4",
    },
    {
      source_id: "section_2.inner_section_2.task_4",
      target_id: "section_2.downstream_join_id",
    },
    {
      source_id: "section_2.task_1",
      target_id: "section_2.downstream_join_id",
    },
    {
      source_id: "section_2.upstream_join_id",
      target_id: "section_2.inner_section_2.task_2",
    },
    {
      source_id: "section_2.upstream_join_id",
      target_id: "section_2.inner_section_2.task_3",
    },
    {
      source_id: "section_2.upstream_join_id",
      target_id: "section_2.task_1",
    },
    {
      label: "I am a realllllllllllllllllly long label",
      source_id: "start",
      target_id: "section_1.upstream_join_id",
    },
  ],
  nodes: [
    {
      id: "end",
      label: "end",
      type: "task",
    },
    {
      children: [
        {
          id: "section_1.normal",
          label: "normal",
          type: "task",
        },
        {
          id: "section_1.taskgroup_setup",
          label: "taskgroup_setup",
          setup_teardown_type: "setup",
          type: "task",
        },
        {
          id: "section_1.taskgroup_teardown",
          label: "taskgroup_teardown",
          setup_teardown_type: "teardown",
          type: "task",
        },
        {
          id: "section_1.upstream_join_id",
          label: "",
          type: "join",
        },
        {
          id: "section_1.downstream_join_id",
          label: "",
          type: "join",
        },
      ],
      id: "section_1",
      is_mapped: false,
      label: "section_1",
      tooltip: "Tasks for section_1",
      type: "task",
    },
    {
      children: [
        {
          children: [
            {
              id: "section_2.inner_section_2.task_2",
              label: "task_2",
              type: "task",
            },
            {
              id: "section_2.inner_section_2.task_3",
              is_mapped: true,
              label: "task_3",
              type: "task",
            },
            {
              id: "section_2.inner_section_2.task_4",
              label: "task_4",
              type: "task",
            },
          ],
          id: "section_2.inner_section_2",
          label: "inner_section_2",
          tooltip: "Tasks for inner_section2",
          type: "task",
        },
        {
          id: "section_2.task_1",
          is_mapped: true,
          label: "task_1",
          type: "task",
        },
        {
          id: "section_2.upstream_join_id",
          label: "",
          type: "join",
        },
        {
          id: "section_2.downstream_join_id",
          label: "",
          type: "join",
        },
      ],
      id: "section_2",
      is_mapped: false,
      label: "section_2",
      tooltip: "Tasks for section_2",
      type: "task",
    },
    {
      id: "start",
      label: "start",
      type: "task",
    },
  ],
};
