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
import type { AssetLineageGraphResponse, AssetResponse } from "openapi/requests/types.gen";

import { getHighlightedLineage } from "./lineageHighlightUtils";

type AssetDescriptor = Pick<AssetResponse, "extra" | "group" | "name" | "uri">;
type AssetLineageNode = AssetLineageGraphResponse["nodes"][number];

export type AssetTypeLabel =
  | "Asset"
  | "BigQuery table"
  | "CSV file"
  | "Dashboard"
  | "Postgres table"
  | "Report"
  | "S3 object"
  | "Snowflake table";

export type LinkedItem = {
  id: string;
  label: string;
  to: string;
};

export type AssetImpactSummary = {
  dags: Array<LinkedItem>;
  dashboards: Array<LinkedItem>;
  reports: Array<LinkedItem>;
  tasks: Array<LinkedItem>;
};

const normalize = (value: unknown) => (typeof value === "string" ? value.toLowerCase() : "");

const getAssetHints = ({ extra, group, name, uri }: AssetDescriptor) =>
  [
    normalize(extra?.asset_type),
    normalize(extra?.type),
    normalize(extra?.kind),
    normalize(extra?.resource_type),
    normalize(group),
    normalize(name),
    normalize(uri),
  ].filter(Boolean);

const hasHint = (hints: Array<string>, pattern: string) => hints.some((hint) => hint.includes(pattern));

const getAssetNodeLink = (nodeId: string) => `/assets/${nodeId.replace("asset:", "")}`;

const getDagNodeLink = (nodeId: string) => `/dags/${nodeId.replace("dag:", "")}`;

const getTaskNodeLink = (nodeId: string) => {
  const [taskPrefix, dagId, ...taskIdParts] = nodeId.split(":");

  void taskPrefix;

  return `/dags/${dagId}/tasks/${taskIdParts.join(":")}`;
};

const getTaskDagId = (nodeId: string) => nodeId.split(":")[1] ?? "";

export const getAssetType = (asset: AssetDescriptor): AssetTypeLabel => {
  const uri = normalize(asset.uri);
  const hints = getAssetHints(asset);

  if (uri.startsWith("snowflake://") || hasHint(hints, "snowflake")) {
    return "Snowflake table";
  }

  if (uri.startsWith("bigquery://") || uri.startsWith("bq://") || hasHint(hints, "bigquery")) {
    return "BigQuery table";
  }

  if (uri.startsWith("postgres://") || uri.startsWith("postgresql://") || hasHint(hints, "postgres")) {
    return "Postgres table";
  }

  if (uri.startsWith("s3://") || hasHint(hints, "s3")) {
    return "S3 object";
  }

  if (uri.startsWith("dashboard://") || hasHint(hints, "dashboard")) {
    return "Dashboard";
  }

  if (uri.startsWith("report://") || hasHint(hints, "report")) {
    return "Report";
  }

  if (uri.endsWith(".csv") || hasHint(hints, ".csv") || hasHint(hints, "csv")) {
    return "CSV file";
  }

  return "Asset";
};

export const getAssetTypeColorPalette = (assetType: AssetTypeLabel) => {
  switch (assetType) {
    case "Asset":
      return "gray";
    case "BigQuery table":
      return "cyan";
    case "CSV file":
      return "green";
    case "Dashboard":
      return "purple";
    case "Postgres table":
      return "blue";
    case "Report":
      return "orange";
    case "S3 object":
      return "teal";
    case "Snowflake table":
      return "blue";
    default:
      return "gray";
  }
};

export const getNodeAssetType = (node: AssetLineageNode): AssetTypeLabel =>
  getAssetType({
    extra: undefined,
    group: node.group ?? "",
    name: node.name,
    uri: node.uri ?? node.name,
  });

export const getAssetImpactSummary = (
  assetId: number | undefined,
  lineageData: AssetLineageGraphResponse | undefined,
): AssetImpactSummary => {
  if (assetId === undefined || lineageData === undefined) {
    return { dags: [], dashboards: [], reports: [], tasks: [] };
  }

  const rootNodeId = `asset:${assetId}`;
  const { highlightedNodeIds } = getHighlightedLineage({
    direction: "downstream",
    edges: lineageData.edges,
    nodeId: rootNodeId,
  });

  const impactedNodes = lineageData.nodes.filter(
    (node) => node.id !== rootNodeId && highlightedNodeIds.has(node.id),
  );

  const tasks = impactedNodes
    .filter((node) => node.node_type === "task")
    .map((node) => ({ id: node.id, label: node.name, to: getTaskNodeLink(node.id) }));
  const dagIds = new Set(tasks.map((task) => getTaskDagId(task.id)));
  const dags = lineageData.nodes
    .filter((node) => node.node_type === "dag" && dagIds.has(node.name))
    .map((node) => ({ id: node.id, label: node.name, to: getDagNodeLink(node.id) }));
  const assetNodes = impactedNodes.filter((node) => node.node_type === "asset");
  const dashboards = assetNodes
    .filter((node) => getNodeAssetType(node) === "Dashboard")
    .map((node) => ({ id: node.id, label: node.name, to: getAssetNodeLink(node.id) }));
  const reports = assetNodes
    .filter((node) => getNodeAssetType(node) === "Report")
    .map((node) => ({ id: node.id, label: node.name, to: getAssetNodeLink(node.id) }));

  return { dags, dashboards, reports, tasks };
};
