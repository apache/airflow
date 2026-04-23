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
import { Box, Flex, Heading, Link, SimpleGrid, Text, VStack } from "@chakra-ui/react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type {
  AssetResponse,
  DagScheduleAssetReference,
  TaskInletAssetReference,
  TaskOutletAssetReference,
} from "openapi/requests/types.gen";
import { Stat } from "src/components/Stat";
import { Tag } from "src/components/ui/Tag";
import { useAssetLineage } from "src/queries/useAssetLineage";

import {
  getAssetImpactSummary,
  getAssetType,
  getAssetTypeColorPalette,
  type LinkedItem,
} from "./assetInsightsUtils";

const Section = ({ children, title }: { readonly children: React.ReactNode; readonly title: string }) => (
  <Box borderColor="border.emphasized" borderRadius={8} borderWidth={1} p={3}>
    <Heading mb={3} size="sm">
      {title}
    </Heading>
    {children}
  </Box>
);

const LinkedList = ({
  emptyMessage,
  items,
}: {
  readonly emptyMessage: string;
  readonly items: Array<LinkedItem>;
}) => {
  if (items.length === 0) {
    return <Text color="fg.muted">{emptyMessage}</Text>;
  }

  return (
    <VStack align="stretch" gap={1}>
      {items.map((item) => (
        <Link asChild color="fg.info" key={item.id}>
          <RouterLink to={item.to}>{item.label}</RouterLink>
        </Link>
      ))}
    </VStack>
  );
};

const AssetReferenceList = ({
  dependencies,
  emptyMessage,
  type,
}: {
  readonly dependencies: Array<
    DagScheduleAssetReference | TaskInletAssetReference | TaskOutletAssetReference
  >;
  readonly emptyMessage: string;
  readonly type: "dag" | "task";
}) => {
  const items = dependencies.map((dependency) =>
    type === "dag"
      ? {
          id: dependency.dag_id,
          label: dependency.dag_id,
          to: `/dags/${dependency.dag_id}`,
        }
      : {
          id: `${dependency.dag_id}-${"task_id" in dependency ? dependency.task_id : ""}`,
          label: `${dependency.dag_id}.${"task_id" in dependency ? dependency.task_id : ""}`,
          to: `/dags/${dependency.dag_id}/tasks/${"task_id" in dependency ? dependency.task_id : ""}`,
        },
  );

  return <LinkedList emptyMessage={emptyMessage} items={items} />;
};

export const AssetInsightsPanel = ({ asset }: { readonly asset?: AssetResponse }) => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const assetId = asset?.id;
  const { data: lineageData } = useAssetLineage(assetId === undefined ? undefined : String(assetId), {
    depth: 10,
  });

  const assetType = useMemo(
    () => (asset === undefined || assetId === undefined ? "Asset" : getAssetType(asset)),
    [asset, assetId],
  );
  const impactSummary = useMemo(() => getAssetImpactSummary(assetId, lineageData), [assetId, lineageData]);

  if (asset === undefined) {
    return undefined;
  }

  return (
    <VStack align="stretch" gap={3} mb={3} mt={3} px={3}>
      <Section title={translate("assets:metadata", { defaultValue: "Airflow Metadata" })}>
        <VStack align="stretch" gap={3}>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:producingTasks", { defaultValue: "Producing Tasks" })}
            </Text>
            <AssetReferenceList
              dependencies={asset.producing_tasks}
              emptyMessage={translate("assets:no_producing_tasks", { defaultValue: "No producing tasks." })}
              type="task"
            />
          </Box>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:consumingTasks", { defaultValue: "Consuming Tasks" })}
            </Text>
            <AssetReferenceList
              dependencies={asset.consuming_tasks}
              emptyMessage={translate("assets:no_consuming_tasks", { defaultValue: "No consuming tasks." })}
              type="task"
            />
          </Box>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:scheduledDags", { defaultValue: "Scheduled DAGs" })}
            </Text>
            <AssetReferenceList
              dependencies={asset.scheduled_dags}
              emptyMessage={translate("assets:no_scheduled_dags", {
                defaultValue: "No asset-scheduled DAGs.",
              })}
              type="dag"
            />
          </Box>
        </VStack>
      </Section>

      <Section title={translate("assets:asset_type", { defaultValue: "Asset Type" })}>
        <Flex align="center" gap={3} justify="space-between">
          <Tag colorPalette={getAssetTypeColorPalette(assetType)} size="lg" variant="subtle">
            {assetType}
          </Tag>
          <Text color="fg.muted" fontSize="sm">
            {asset.uri}
          </Text>
        </Flex>
      </Section>

      <Section title={translate("assets:impact_analysis", { defaultValue: "Impact Analysis" })}>
        <Text color="fg.muted" fontSize="sm" mb={3}>
          {translate("assets:impact_analysis_note", {
            defaultValue: "Static downstream reachability from the current lineage graph.",
          })}
        </Text>

        <SimpleGrid columns={2} gap={4} mb={4}>
          <Stat label={translate("assets:impacted_dags", { defaultValue: "Potential DAGs" })}>
            <Text fontSize="lg" fontWeight="semibold">
              {impactSummary.dags.length}
            </Text>
          </Stat>
          <Stat label={translate("assets:impacted_tasks", { defaultValue: "Potential Tasks" })}>
            <Text fontSize="lg" fontWeight="semibold">
              {impactSummary.tasks.length}
            </Text>
          </Stat>
          <Stat label={translate("assets:impacted_reports", { defaultValue: "Potential Reports" })}>
            <Text fontSize="lg" fontWeight="semibold">
              {impactSummary.reports.length}
            </Text>
          </Stat>
          <Stat label={translate("assets:impacted_dashboards", { defaultValue: "Potential Dashboards" })}>
            <Text fontSize="lg" fontWeight="semibold">
              {impactSummary.dashboards.length}
            </Text>
          </Stat>
        </SimpleGrid>

        <VStack align="stretch" gap={3}>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:downstream_dags", { defaultValue: "Downstream DAGs" })}
            </Text>
            <LinkedList
              emptyMessage={translate("assets:no_downstream_dags", { defaultValue: "No downstream DAGs." })}
              items={impactSummary.dags}
            />
          </Box>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:downstream_tasks", { defaultValue: "Downstream Tasks" })}
            </Text>
            <LinkedList
              emptyMessage={translate("assets:no_downstream_tasks", { defaultValue: "No downstream tasks." })}
              items={impactSummary.tasks}
            />
          </Box>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:downstream_reports", { defaultValue: "Downstream Reports" })}
            </Text>
            <LinkedList
              emptyMessage={translate("assets:no_downstream_reports", {
                defaultValue: "No downstream reports.",
              })}
              items={impactSummary.reports}
            />
          </Box>
          <Box>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("assets:downstream_dashboards", { defaultValue: "Downstream Dashboards" })}
            </Text>
            <LinkedList
              emptyMessage={translate("assets:no_downstream_dashboards", {
                defaultValue: "No downstream dashboards.",
              })}
              items={impactSummary.dashboards}
            />
          </Box>
        </VStack>
      </Section>
    </VStack>
  );
};
