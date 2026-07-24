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
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { DeleteDagButton } from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
import { TogglePause } from "src/components/TogglePause";
import { TriggerDAGButton } from "src/components/TriggerDag/TriggerDAGButton";

export const DagCardActions = ({ dag }: { readonly dag: DAGWithLatestDagRunsResponse }) => (
  <>
    <NeedsReviewBadge pendingActions={dag.pending_actions} />
    <TogglePause dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} isPaused={dag.is_paused} />
    <TriggerDAGButton
      allowedRunTypes={dag.allowed_run_types}
      dagDisplayName={dag.dag_display_name}
      dagId={dag.dag_id}
      isPaused={dag.is_paused}
    />
    <FavoriteDagButton dagId={dag.dag_id} isFavorite={dag.is_favorite} />
    <DeleteDagButton dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} />
  </>
);
