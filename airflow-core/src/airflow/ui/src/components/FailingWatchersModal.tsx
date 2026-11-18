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
import { useState } from "react";

import { Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiZap } from "react-icons/fi";

import { useTriggerServiceGetTriggers } from "openapi/queries";
import type {
  GetTriggersData,
  TriggerAssetWatcherReferenceResponse,
  TriggerDetailResponse,
} from "openapi/requests/types.gen";

import { Modal, RouterLink } from "src/system-components";

import { useConfig } from "src/queries/useConfig";

import { DataTable } from "./DataTable";
import type { CardDef } from "./DataTable/types";
import { StatusMessage } from "./StatusMessage";
import Time from "./Time";

type FailingWatcher = {
  readonly trigger: TriggerDetailResponse;
  readonly watcher: TriggerAssetWatcherReferenceResponse;
};

type Props = {
  readonly dagId: string;
  readonly onClose: () => void;
  readonly open: boolean;
};

/** The failing watchers' triggers on the assets the Dag is scheduled on, narrowed to the Dag on the server. */
export const getFailingWatchersParams = (dagId: string) =>
  ({ kind: ["asset_watcher"], scheduledDagId: dagId, status: ["failing"] }) satisfies GetTriggersData;

/** A failing watcher, stated like an import error in DAGWarningsModal: when, why, and its trigger for the rest. */
const FailingWatcherMessage = ({ trigger, watcher }: FailingWatcher) => {
  const { t: translate } = useTranslation(["components", "browse"]);

  return (
    <StatusMessage
      body={
        <>
          <HStack color="fg.muted" flexWrap="wrap" fontSize="sm" gap={4} mb={2}>
            {trigger.first_failed_at === null ? undefined : (
              <Text>
                {translate("browse:triggers.detail.firstFailedAt")}
                {": "}
                <Time datetime={trigger.first_failed_at} />
              </Text>
            )}
            {trigger.last_failed_at === null ? undefined : (
              <Text>
                {translate("browse:triggers.detail.lastFailedAt")}
                {": "}
                <Time datetime={trigger.last_failed_at} />
              </Text>
            )}
          </HStack>
          {trigger.last_error === null ? undefined : (
            <Text color="fg.error" fontSize="sm" mb={2} whiteSpace="pre-wrap" wordBreak="break-word">
              <code>{trigger.last_error}</code>
            </Text>
          )}
          <RouterLink fontSize="sm" to={`/triggers/${trigger.id}`}>
            {translate("dagWarnings.viewTrigger")}
          </RouterLink>
        </>
      }
      heading={translate("dagWarnings.failingWatcher", {
        assetName: watcher.asset_name,
        watcherName: watcher.watcher_name,
      })}
      severity="error"
    />
  );
};

// Scheduled on the asset directly or by an asset name or URI reference; not producing or consuming it in a task.
const cardDef = (dagId: string): CardDef<TriggerDetailResponse> => ({
  card: ({ row: trigger }) => (
    <VStack align="stretch">
      {trigger.asset_watchers
        .filter((watcher) => watcher.scheduled_dag_ids.includes(dagId))
        .map((watcher) => (
          <FailingWatcherMessage
            key={`${watcher.asset_id}-${watcher.watcher_name}`}
            trigger={trigger}
            watcher={watcher}
          />
        ))}
    </VStack>
  ),
});

/** The failing watchers that hold a Dag up, in the same modal as the Dag's import error and warnings. */
export const FailingWatchersModal = ({ dagId, onClose, open }: Props) => {
  const { t: translate } = useTranslation("dag");
  const pageSize = useConfig("fallback_page_limit") as number;
  const [pageIndex, setPageIndex] = useState(0);
  const { data, isLoading } = useTriggerServiceGetTriggers(
    { ...getFailingWatchersParams(dagId), limit: pageSize, offset: pageIndex * pageSize },
    undefined,
    { enabled: open, placeholderData: (prev) => prev },
  );
  const total = data?.total_entries ?? 0;

  const handleOpenChange = () => {
    setPageIndex(0);
    onClose();
  };

  return (
    <Modal
      headerProps={{
        children: (
          <HStack gap={2}>
            <FiZap />
            <Heading fontSize="lg">{translate("failingWatcherTriggers", { count: total })}</Heading>
          </HStack>
        ),
      }}
      onOpenChange={handleOpenChange}
      open={open}
      scrollBehavior="inside"
    >
      <DataTable
        cardDef={cardDef(dagId)}
        columns={[]}
        data={data?.triggers ?? []}
        displayMode="card"
        hideRowCountHeading
        initialState={{ pagination: { pageIndex, pageSize }, sorting: [] }}
        isLoading={isLoading}
        modelName="common:trigger"
        onStateChange={(state) => setPageIndex(state.pagination.pageIndex)}
        total={total}
      />
    </Modal>
  );
};
