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
import { useTranslation } from "react-i18next";
import { FiZap } from "react-icons/fi";
import { useNavigate } from "react-router-dom";

import { useDagServiceGetDagDetails, useTriggerServiceGetTriggers } from "openapi/queries";
import type { TriggerAssetWatcherReferenceResponse, TriggerDetailResponse } from "openapi/requests/types.gen";

import { StatusChip } from "src/components/StatusChip";

type Props = {
  /** Whether a watcher of a failing trigger is one this chip speaks for. */
  readonly isCounted: (watcher: TriggerAssetWatcherReferenceResponse) => boolean;
  readonly triggers?: Array<TriggerDetailResponse>;
};

/**
 * Failing watchers, as a red status chip. It opens the watcher's trigger, or the failing watchers in
 * the Triggers list when there are several.
 */
export const FailingWatchersChip = ({ isCounted, triggers }: Props) => {
  const { t: translate } = useTranslation("dag");
  const navigate = useNavigate();
  const failing = (triggers ?? []).flatMap((trigger) =>
    trigger.asset_watchers.filter(isCounted).map(() => trigger.id),
  );
  const triggerIds = [...new Set(failing)];

  if (failing.length === 0) {
    return undefined;
  }

  return (
    <StatusChip
      icon={<FiZap />}
      label={translate("failingWatchers", { count: failing.length })}
      onClick={() =>
        void navigate(
          triggerIds.length === 1
            ? `/triggers/${triggerIds[0]}`
            : "/triggers?kind=asset_watcher&status=failing",
        )
      }
      severity="error"
    />
  );
};

/** Failing watchers on the assets the Dag is scheduled on, which keep it from running. */
export const DagFailingWatchersChip = ({ dagId }: { readonly dagId: string }) => {
  // The details the breadcrumb already loads: only a Dag scheduled on assets asks for its watchers.
  const { data: dag } = useDagServiceGetDagDetails({ dagId }, undefined, { enabled: dagId !== "" });
  // Narrowed to this Dag on the server, so the page limit is per Dag.
  const { data } = useTriggerServiceGetTriggers(
    { kind: ["asset_watcher"], limit: 100, scheduledDagId: dagId, status: ["failing"] },
    undefined,
    { enabled: Boolean(dag?.asset_expression) },
  );

  // Scheduled on the asset directly or by an asset name or URI reference; not producing or consuming it in a task.
  return (
    <FailingWatchersChip
      isCounted={(watcher) => watcher.scheduled_dag_ids.includes(dagId)}
      triggers={data?.triggers}
    />
  );
};
