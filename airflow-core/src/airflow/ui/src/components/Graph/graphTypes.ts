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
import { AliasNode } from "./AliasNode";
import { AssetConditionNode } from "./AssetConditionNode";
import { AssetNode } from "./AssetNode";
import { DagNode } from "./DagNode";
import { DefaultNode } from "./DefaultNode";
import Edge from "./Edge";
import { JoinNode } from "./JoinNode";
import { TaskNode } from "./TaskNode";

export const nodeTypes = {
  asset: AssetNode,
  "asset-alias": AliasNode,
  "asset-condition": AssetConditionNode,
  "asset-name-ref": DefaultNode,
  "asset-uri-ref": DefaultNode,
  dag: DagNode,
  join: JoinNode,
  task: TaskNode,
};

export const edgeTypes = { custom: Edge };
